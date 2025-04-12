import static org.junit.Assert.*;
import org.junit.Test;

import protocol.ProtocolLayer;
import protocol.ProtocolLayer.ProtocolCallback;
import client.Message;
import protocol.MessageFragmentBuffer;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;

/**
 * SimulatedNetworkTest creates a virtual network of four nodes arranged in a line.
 * The connectivity is:
 *   - Node 1: [2]
 *   - Node 2: [1,3]
 *   - Node 3: [2,4]
 *   - Node 4: [3]
 *
 * The test waits for neighbor discovery and DV convergence (routing table buildup),
 * then sends a broadcast message (should be delivered to every node) and a whisper
 * message from Node 1 to Node 4 (only delivered to Node 4).
 */
public class SimulatedNetworkTest {

    // Helper class representing a simulated node.
    private static class NodeSim {
        public int address;
        public BlockingQueue<Message> recvQueue;
        public BlockingQueue<Message> sendQueue;
        public ProtocolLayer layer;
        // List of messages received (by the ProtocolCallback).
        public List<String> receivedMessages = Collections.synchronizedList(new ArrayList<>());

        public NodeSim(int address) {
            this.address = address;
            recvQueue = new LinkedBlockingQueue<>();
            sendQueue = new LinkedBlockingQueue<>();
            // Create a callback that adds delivered messages to our list.
            ProtocolCallback cb = new ProtocolCallback() {
                @Override
                public void onReceivedChatMessage(String fromAddr, String message) {
                    String full = "From " + fromAddr + ": " + message;
                    receivedMessages.add(full);
                    System.out.println("Node " + address + " received: " + full);
                }
                @Override
                public void onNeighborsChanged(Set<Byte> currentNeighbors) {
                    System.out.println("Node " + address + " neighbors: " + currentNeighbors);
                }
            };
            // Create ProtocolLayer with a static address.
            layer = new ProtocolLayer((byte) address, recvQueue, sendQueue, cb);
        }

        // For testing, use reflection to access the internal routing table.
        // We return a Map from destination (Byte) to an int array [distance, nextHop].
        public Map<Byte, int[]> getRoutingTable() {
            try {
                java.lang.reflect.Field rtField = layer.getClass().getDeclaredField("routingTable");
                rtField.setAccessible(true);
                Map<Byte, Object> rt = (Map<Byte, Object>) rtField.get(layer);
                Map<Byte, int[]> result = new HashMap<>();
                for (Map.Entry<Byte, Object> entry : rt.entrySet()) {
                    Object routeInfo = entry.getValue();
                    java.lang.reflect.Field distField = routeInfo.getClass().getDeclaredField("distance");
                    java.lang.reflect.Field nextHopField = routeInfo.getClass().getDeclaredField("nextHop");
                    distField.setAccessible(true);
                    nextHopField.setAccessible(true);
                    int distance = distField.getInt(routeInfo);
                    byte nextHop = nextHopField.getByte(routeInfo);
                    result.put(entry.getKey(), new int[]{distance, nextHop});
                }
                return result;
            } catch (Exception e) {
                e.printStackTrace();
                return Collections.emptyMap();
            }
        }
    }

    /**
     * Dispatcher simulates the wireless medium.
     * For each node, it polls its sendQueue and delivers messages to neighbors
     * based on the connectivity map.
     */
    private static class Dispatcher implements Runnable {
        // Map from node address to NodeSim.
        private Map<Integer, NodeSim> nodes;
        // Connectivity: for each node, list of addresses it can directly reach.
        private Map<Integer, Set<Integer>> connectivity;
        private volatile boolean running = true;

        public Dispatcher(Map<Integer, NodeSim> nodes, Map<Integer, Set<Integer>> connectivity) {
            this.nodes = nodes;
            this.connectivity = connectivity;
        }

        @Override
        public void run() {
            while (running) {
                for (NodeSim sender : nodes.values()) {
                    Message msg = sender.sendQueue.poll();
                    if (msg != null) {
                        ByteBuffer buf = msg.getData();
                        buf.rewind();
                        if (buf.remaining() < 8) continue;
                        // We assume header field [3] is nextHop.
                        byte nextHop = buf.get(3);
                        int senderAddr = sender.address;
                        Set<Integer> nbrs = connectivity.get(senderAddr);
                        if (nextHop == (byte) 0xFF) { // Broadcast frame.
                            for (int nbr : nbrs) {
                                Message copy = new Message(msg.getType(), cloneByteBuffer(buf));
                                nodes.get(nbr).recvQueue.offer(copy);
                            }
                        } else {
                            if (nbrs.contains((int) nextHop)) {
                                Message copy = new Message(msg.getType(), cloneByteBuffer(buf));
                                nodes.get((int) nextHop).recvQueue.offer(copy);
                            }
                        }
                    }
                }
                try {
                    Thread.sleep(5);
                } catch (InterruptedException e) {
                    running = false;
                }
            }
        }

        public void shutdown() {
            running = false;
        }

        private ByteBuffer cloneByteBuffer(ByteBuffer src) {
            ByteBuffer clone = ByteBuffer.allocate(src.capacity());
            src.rewind();
            clone.put(src);
            clone.flip();
            return clone;
        }
    }

    @Test
    public void testRoutingAndMessageDelivery() throws Exception {
        // Create 4 nodes with addresses 1 to 4.
        Map<Integer, NodeSim> nodes = new HashMap<>();
        for (int i = 1; i <= 4; i++) {
            nodes.put(i, new NodeSim(i));
        }

        // Define connectivity as:
        // Node 1: [2]
        // Node 2: [1,3]
        // Node 3: [2,4]
        // Node 4: [3]
        Map<Integer, Set<Integer>> connectivity = new HashMap<>();
        connectivity.put(1, new HashSet<>(Arrays.asList(2)));
        connectivity.put(2, new HashSet<>(Arrays.asList(1, 3)));
        connectivity.put(3, new HashSet<>(Arrays.asList(2, 4)));
        connectivity.put(4, new HashSet<>(Arrays.asList(3)));

        // Start ProtocolLayer for each node.
        for (NodeSim ns : nodes.values()) {
            ns.layer.start();
        }

        // Start the dispatcher that simulates the wireless medium.
        Dispatcher dispatcher = new Dispatcher(nodes, connectivity);
        Thread dispThread = new Thread(dispatcher, "DispatcherThread");
        dispThread.start();

        // Wait for enough time so that nodes discover their direct neighbors.
        // For DV routing to converge (neighbors + routes to all nodes), wait about 60 seconds.
        Thread.sleep(60000);

        // --- Verify Routing Tables ---
        // For example, we expect Node 1 routing table to include:
        //   * Route to Node 2: distance 1, nextHop = 2
        //   * Route to Node 3: distance 2, nextHop = 2
        //   * Route to Node 4: distance 3, nextHop = 2
        NodeSim node1 = nodes.get(1);
        Map<Byte, int[]> rt1 = node1.getRoutingTable();
        assertTrue("Node 1 routing table should contain Node 2", rt1.containsKey((byte)2));
        assertTrue("Node 1 routing table should contain Node 3", rt1.containsKey((byte)3));
        assertTrue("Node 1 routing table should contain Node 4", rt1.containsKey((byte)4));

        int[] routeTo2 = rt1.get((byte)2);
        int[] routeTo3 = rt1.get((byte)3);
        int[] routeTo4 = rt1.get((byte)4);
        assertEquals("Node 1: distance to Node 2 should be 1", 1, routeTo2[0]);
        assertEquals("Node 1: nextHop to Node 2 should be 2", 2, routeTo2[1]);
        assertEquals("Node 1: distance to Node 3 should be 2", 2, routeTo3[0]);
        assertEquals("Node 1: nextHop to Node 3 should be 2", 2, routeTo3[1]);
        assertEquals("Node 1: distance to Node 4 should be 3", 3, routeTo4[0]);
        assertEquals("Node 1: nextHop to Node 4 should be 2", 2, routeTo4[1]);

        // (You can print out other nodes' routing tables similarly.)

        // --- Test Broadcast ---
        // Send a broadcast message from Node 2.
        nodes.get(2).layer.sendChatMessage("Broadcast message from Node 2", ProtocolLayer.BROADCAST_ADDR);
        Thread.sleep(3000);

        // Verify that all nodes (1-4) have received the broadcast message.
        for (int i = 1; i <= 4; i++) {
            NodeSim ns = nodes.get(i);
            boolean received = false;
            for (String msg : ns.receivedMessages) {
                if (msg.contains("Broadcast message from Node 2")) {
                    received = true;
                    break;
                }
            }
            assertTrue("Node " + i + " should have received the broadcast message", received);
        }

        // --- Test Whisper (Unicast) ---
        // Send a unicast (whisper) message from Node 1 to Node 4.
        nodes.get(1).layer.sendChatMessage("Whisper from Node 1 to Node 4", (byte)4);
        Thread.sleep(3000);

        // Verify that only Node 4 received the whisper message.
        for (int i = 1; i <= 4; i++) {
            NodeSim ns = nodes.get(i);
            boolean received = false;
            for (String msg : ns.receivedMessages) {
                if (msg.contains("Whisper from Node 1 to Node 4")) {
                    received = true;
                    break;
                }
            }
            if (i == 4) {
                assertTrue("Node 4 should receive the whisper message", received);
            } else {
                assertFalse("Node " + i + " should not receive the whisper message", received);
            }
        }

        // Shutdown dispatcher.
        dispatcher.shutdown();
        dispThread.interrupt();
    }
}