package protocol;

import client.Message;
import client.MessageType;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.Set;
import java.util.Map;

/**
 * ProtocolLayer with:
 *  1) Dynamic Addressing,
 *  2) MAC layer using CSMA/CA
 *  3) Stop‐and‐Wait reliability (fragmentation + ACK),
 *  4) Distance Vector routing for multi‐hop unicast forwarding, and
 *  5) An 8‐byte header (carrying both the final destination and link‐layer next hop).
 *
 */
public class ProtocolLayer {

    // ============ Constants ============
    public static final byte BROADCAST_ADDR = (byte) 0xFF;

    private static final int MAX_DATA_LEN = 32;
    // Header layout:
    // [0] controlFlags, [1] srcAddr, [2] finalDest, [3] nextHop, [4..5] messageID, [6] fragIndex, [7] totalFrags
    private static final int HEADER_LEN = 8;

    // Control flags
    private static final byte FLAG_DATA          = 0x00;
    private static final byte FLAG_HELLO         = 0x01;
    private static final byte FLAG_ACK           = 0x02;
    private static final byte FLAG_ADDR_CLAIM    = 0x03;
    private static final byte FLAG_ADDR_CONFLICT = 0x04;
    private static final byte FLAG_DV            = 0x05;
    private static final byte FLAG_INTENT        = 0x0E;

    // When a static address is provided (nonzero), dynamic addressing is skipped.
    private volatile boolean isAddressAssigned = false;

    private volatile byte localAddress;
    private BlockingQueue<Message> recvQueue;   // From Client.Listener.
    private BlockingQueue<Message> sendQueue;   // To Client.Sender.

    // MAC transmission queue (stores MacFrame objects).
    private BlockingQueue<MacFrame> macTxQueue = new LinkedBlockingQueue<>();

    // Channel state from BUSY/FREE messages.
    private volatile boolean channelBusy = false;

    // Neighbors (directly connected nodes): maps neighbor address to last-heard time in ms.
    private ConcurrentHashMap<Byte, Long> neighbors = new ConcurrentHashMap<>();
    private static final long NEIGHBOR_TIMEOUT_MS = 60000;

    // Fragment reassembly storage.
    private Map<String, MessageFragmentBuffer> reassemblyMap = new ConcurrentHashMap<>();
    private Set<String> seenFragmentIds = Collections.newSetFromMap(new ConcurrentHashMap<>());

    // Callback interface for delivering complete messages.
    public interface ProtocolCallback {
        void onReceivedChatMessage(String fromAddr, String message);
        void onNeighborsChanged(Set<Byte> currentNeighbors);
    }
    private ProtocolCallback callback;

    // Stop-and-Wait ACK manager.
    private AckManager ackManager = new AckManager();

    // For dynamic addressing.
    private final Object addrLock = new Object();
    private boolean hasConflict = false;
    private byte localConflictCandidate = 0;
    private static final int ADDR_ASSIGN_TIMEOUT_MS = 3000;

    // Distance Vector Routing
    private static final int INFINITY = 999;
    // Routing table: destination -> (distance, nextHop)
    private Map<Byte, RouteInfo> routingTable = new ConcurrentHashMap<>();
    private static final long DV_BROADCAST_INTERVAL_MS = 20000;

    private static class RouteInfo {
        public int distance;
        public byte nextHop;
        public RouteInfo(int distance, byte nextHop) {
            this.distance = distance;
            this.nextHop = nextHop;
        }
    }

    // MacFrame Class
    private static class MacFrame {
        ByteBuffer frame;
        public MacFrame(ByteBuffer frame) {
            this.frame = frame;
        }
    }

    public ProtocolLayer(byte initialAddress,
                         BlockingQueue<Message> recvQueue,
                         BlockingQueue<Message> sendQueue,
                         ProtocolCallback callback) {
        this.localAddress = initialAddress;
        this.recvQueue = recvQueue;
        this.sendQueue = sendQueue;
        this.callback = callback;

        if (this.localAddress != 0) {
            isAddressAssigned = true;
            routingTable.put(localAddress, new RouteInfo(0, localAddress));
        }
    }

    public void start() {
        new Thread(this::receiveLoop, "Protocol-ReceiveLoop").start();
        new Thread(this::macLoop, "Protocol-MacLoop").start();
        if (!isAddressAssigned) {
            new Thread(this::addressAssignmentLoop, "AddressAssignmentLoop").start();
        }
        new Thread(this::helloLoop, "Protocol-HelloLoop").start();
        new Thread(this::dvLoop, "Protocol-DVLoop").start();
    }

    public byte getLocalAddress() {
        return localAddress;
    }

    public void sendChatMessage(String message, byte finalDest) {
        if (!isAddressAssigned) {
            System.out.println("Address not assigned yet, cannot send messages.");
            return;
        }
        new Thread(() -> {
            try {
                stopAndWaitSend(message, finalDest);
            } catch (InterruptedException e) {
                System.err.println("sendChatMessage interrupted: " + e);
            }
        }, "StopAndWaitSender").start();
    }


    // Dynamic Addressing
    private void addressAssignmentLoop() {
        Random rand = new Random();
        while (!isAddressAssigned) {
            byte candidate = (byte)(1 + rand.nextInt(200));
            System.out.println("Attempting to claim address: " + candidate);
            broadcastAddressClaim(candidate);

            boolean conflict = false;
            long start = System.currentTimeMillis();
            while (System.currentTimeMillis() - start < ADDR_ASSIGN_TIMEOUT_MS) {
                synchronized (addrLock) {
                    if (localConflictCandidate == candidate) {
                        conflict = true;
                        break;
                    }
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    return;
                }
            }
            if (!conflict) {
                synchronized (addrLock) {
                    if (!hasConflict) {
                        localAddress = candidate;
                        isAddressAssigned = true;
                        routingTable.put(localAddress, new RouteInfo(0, localAddress));
                        System.out.println("Assigned address: " + localAddress);
                    } else {
                        System.out.println("Conflict found after waiting, retrying...");
                        hasConflict = false;
                        localConflictCandidate = 0;
                    }
                }
            } else {
                synchronized (addrLock) {
                    System.out.println("Conflict for address " + candidate + ", retrying...");
                    hasConflict = false;
                    localConflictCandidate = 0;
                }
            }
        }
    }

    private void broadcastAddressClaim(byte candidate) {
        ByteBuffer buf = buildFrameHeader(FLAG_ADDR_CLAIM,
                                          (byte)0,
                                          BROADCAST_ADDR,
                                          BROADCAST_ADDR,
                                          (short)0,
                                          (byte)0,
                                          (byte)1);
        buf.put(candidate);
        buf.flip();
        macTxQueue.offer(new MacFrame(buf));
    }

    // Distance Vector Routing
    private void dvLoop() {
        try {
            while (true) {
                Thread.sleep(DV_BROADCAST_INTERVAL_MS);
                if (isAddressAssigned) {
                    broadcastDistanceVector();
                }
            }
        } catch (InterruptedException e) {
            // exit thread
        }
    }

    private void broadcastDistanceVector() {
        List<Byte> payloadList = new ArrayList<>();
        Set<Byte> dests = routingTable.keySet();
        payloadList.add((byte) dests.size());
        for (Byte d : dests) {
            RouteInfo ri = routingTable.get(d);
            int dist = (ri.distance > 255) ? 255 : ri.distance;
            payloadList.add(d);
            payloadList.add((byte) dist);
        }
        byte[] payload = new byte[payloadList.size()];
        for (int i = 0; i < payloadList.size(); i++) {
            payload[i] = payloadList.get(i);
        }
        ByteBuffer buf = buildFrameHeader(FLAG_DV,
                                          localAddress,
                                          BROADCAST_ADDR,
                                          BROADCAST_ADDR,
                                          (short)0,
                                          (byte)0,
                                          (byte)1);
        buf.put(payload);
        buf.flip();
        macTxQueue.offer(new MacFrame(buf));
    }

    private void handleDistanceVector(byte srcAddr, byte[] payload) {
        if (payload.length < 1) return;
        int count = payload[0] & 0xFF;
        if (payload.length < 1 + count * 2) {
            System.out.println("DV parse error: not enough bytes");
            return;
        }
        int distToNeighbor = getDistanceTo(srcAddr);
        if (distToNeighbor == INFINITY) {
            if (neighbors.containsKey(srcAddr)) {
                distToNeighbor = 1;
                updateRoute(srcAddr, 1, srcAddr);
            } else {
                distToNeighbor = 1;
                updateRoute(srcAddr, 1, srcAddr);
            }
        }
        int idx = 1;
        for (int i = 0; i < count; i++){
            byte dest = payload[idx++];
            int theirDist = payload[idx++] & 0xFF;
            int newDist = distToNeighbor + theirDist;
            int oldDist = getDistanceTo(dest);
            if (newDist < oldDist) {
                updateRoute(dest, newDist, srcAddr);
            }
        }
    }

    private int getDistanceTo(byte dest) {
        RouteInfo ri = routingTable.get(dest);
        return (ri == null) ? INFINITY : ri.distance;
    }

    /**
     * Updates the route for destination 'dest' with the given distance and nextHop.
     * If the current stored route has the same distance and nextHop, do nothing.
     */
    private void updateRoute(byte dest, int dist, byte nextHop) {
        RouteInfo current = routingTable.get(dest);
        if (current != null && current.distance == dist && current.nextHop == nextHop) {
            // No change, so do not update or print.
            return;
        }
        routingTable.put(dest, new RouteInfo(dist, nextHop));
        System.out.println("Node " + localAddress + " updated route to " + dest
                                   + ": dist=" + dist + ", nextHop=" + nextHop);
    }

    private void receiveLoop() {
        while (true) {
            try {
                Message msg = recvQueue.take();
                switch (msg.getType()) {
                    case BUSY:
                        channelBusy = true;
                        break;
                    case FREE:
                        channelBusy = false;
                        break;
                    case INTENT:
                        // Process the incoming INTENT frame
                        // Extract a key from the frame so we know if we already relayed it
                        ByteBuffer intentBuf = msg.getData();
                        String intentKey = makeIntentKey(intentBuf);

                        // If we haven't already relayed this INTENT, do so.
                        if (!intentRelayCache.contains(intentKey)) {
                            intentRelayCache.add(intentKey);
                            try {
                                Thread.sleep(1000);
                            } catch (InterruptedException e) {
                                // Ignore interruption for the relay delay
                            }
                            System.out.println("Relaying INTENT from node " + intentBuf.get(1));
                            sendIntentFrame(); // Relay the INTENT frame.
                        }

                        // Mark channel as busy, so we do not attempt to transmit our own frame.
                        channelBusy = true;
                        break;
                    case DATA:
                    case DATA_SHORT:
                        handleIncomingData(msg.getData());
                        break;
                    case SENDING:
                    case DONE_SENDING:
                        break;
                    default:
                        break;
                }
            } catch (InterruptedException e) {
                System.err.println("receiveLoop interrupted: " + e);
                return;
            }
        }
    }

    private void handleIncomingData(ByteBuffer buf) {
        buf.rewind();
        byte[] raw = new byte[buf.remaining()];
        buf.get(raw);
        if (raw.length < HEADER_LEN) return;

        byte controlFlags  = raw[0];
        byte srcAddr       = raw[1];
        byte finalDest     = raw[2];
        byte nextHop       = raw[3];
        short messageID    = (short)(((raw[4] & 0xFF) << 8) | (raw[5] & 0xFF));
        byte fragIndex     = raw[6];
        byte totalFrags    = raw[7];

        int payloadLen = raw.length - HEADER_LEN;
        byte[] payload = new byte[payloadLen];
        System.arraycopy(raw, HEADER_LEN, payload, 0, payloadLen);

        // Only update neighbor information via HELLO messages.
        if (controlFlags == FLAG_HELLO) {
            if (srcAddr != 0) {
                updateNeighbor(srcAddr);
                updateRoute(srcAddr, 1, srcAddr);
            }
        }

        switch (controlFlags) {
            case FLAG_HELLO:
                return;
            case FLAG_ACK:
                handleAckFrame(srcAddr, finalDest, nextHop, messageID, fragIndex);
                return;
            case FLAG_ADDR_CLAIM:
                handleAddrClaim(srcAddr, payload);
                return;
            case FLAG_ADDR_CONFLICT:
                handleAddrConflict(payload);
                return;
            case FLAG_DV:
                handleDistanceVector(srcAddr, payload);
                return;
            default:
                handleNormalData(controlFlags, srcAddr, finalDest, nextHop,
                                 messageID, fragIndex, totalFrags, payload);
        }
    }

    private void handleNormalData(byte controlFlags, byte srcAddr, byte finalDest, byte nextHop,
                                  short messageID, byte fragIndex, byte totalFrags,
                                  byte[] payload) {
        // Broadcast: if nextHop equals BROADCAST_ADDR.
        if (nextHop == BROADCAST_ADDR) {
            String fragKey = srcAddr + ":" + messageID + ":" + fragIndex;
            if (seenFragmentIds.contains(fragKey)) return;
            seenFragmentIds.add(fragKey);
            if (srcAddr != localAddress) {
                forwardBroadcast(controlFlags, srcAddr, finalDest, nextHop,
                                 messageID, fragIndex, totalFrags, payload);
            }
            reassembleAndDeliver(srcAddr, messageID, fragIndex, totalFrags, payload);
            return;
        }

        // Unicast: process only if nextHop equals our address.
        if (nextHop != localAddress) return;

        if (finalDest == localAddress) {
            String fragKey = srcAddr + ":" + messageID + ":" + fragIndex;
            if (!seenFragmentIds.contains(fragKey)) {
                seenFragmentIds.add(fragKey);
                reassembleAndDeliver(srcAddr, messageID, fragIndex, totalFrags, payload);
                if (controlFlags == FLAG_DATA) {
                    // Generate ACK: our address becomes ACK sender; the original sender is the final destination for the ACK.
                    byte ackNextHop = findNextHopFor(srcAddr);
                    if (ackNextHop == 0) {
                        System.out.println("No route for ACK to sender " + srcAddr + ". Using broadcast fallback.");
                        ackNextHop = BROADCAST_ADDR;
                    }
                    sendAckFrame(localAddress, srcAddr, ackNextHop, messageID, fragIndex);
                }
            }
        } else {
            // Intermediate hop: forward unicast.
            byte nextHop2 = findNextHopFor(finalDest);
            if (nextHop2 == 0) {
                System.out.println("No route for finalDest=" + finalDest + " from " + localAddress);
                return;
            }
            forwardUnicast(controlFlags, srcAddr, finalDest, nextHop2,
                           messageID, fragIndex, totalFrags, payload);
        }
    }

    private void forwardBroadcast(byte controlFlags, byte srcAddr, byte finalDest, byte nextHop,
                                  short messageID, byte fragIndex, byte totalFrags,
                                  byte[] payload) {
        ByteBuffer buf = buildFrameHeader(controlFlags, srcAddr, finalDest, BROADCAST_ADDR,
                                          messageID, fragIndex, totalFrags);
        buf.put(payload);
        buf.flip();
        macTxQueue.offer(new MacFrame(buf));
    }

    private void forwardUnicast(byte controlFlags, byte srcAddr, byte finalDest, byte nextHop,
                                short messageID, byte fragIndex, byte totalFrags,
                                byte[] payload) {
        ByteBuffer buf = buildFrameHeader(controlFlags, srcAddr, finalDest, nextHop,
                                          messageID, fragIndex, totalFrags);
        buf.put(payload);
        buf.flip();
        macTxQueue.offer(new MacFrame(buf));
    }

    private byte findNextHopFor(byte dest) {
        RouteInfo ri = routingTable.get(dest);
        if (ri == null || ri.distance >= INFINITY) return 0;
        return ri.nextHop;
    }

    private String decodeMessage(byte[] data) {
        // Determine the effective length by scanning backwards from the end.
        int end = data.length;
        while (end > 0) {
            byte b = data[end - 1];
            // Consider b as padding if it is zero or a whitespace (space, tab) or any control code (except newline or carriage return)
            if (b == 0 || b == ' ' || b == '\t' ||
                    ((b < 32) && (b != '\n' && b != '\r'))) {
                end--;
            } else {
                break;
            }
        }
        return new String(data, 0, end, StandardCharsets.UTF_8);
    }
    private void reassembleAndDeliver(byte srcAddr, short messageID,
                                      byte fragIndex, byte totalFrags, byte[] payload) {
        String mapKey = srcAddr + ":" + messageID;
        MessageFragmentBuffer buf = reassemblyMap.get(mapKey);
        if (buf == null) {
            buf = new MessageFragmentBuffer(totalFrags);
            reassemblyMap.put(mapKey, buf);
        }
        buf.addFragment(fragIndex, payload);
        if (buf.isComplete()) {
            byte[] fullData = buf.reassemble();
            String msgText = decodeMessage(fullData);

            if (srcAddr != localAddress) {
                if (callback != null) {
                    callback.onReceivedChatMessage("Node " + srcAddr, msgText);
                }
            }
            reassemblyMap.remove(mapKey);
        }
    }

    private void handleAckFrame(byte srcAddr, byte finalDest, byte nextHop,
                                short messageID, byte fragIndex) {
        if (nextHop != localAddress) return;
        if (finalDest == localAddress) {
            String ackKey = ackManager.makeAckKey(localAddress, messageID, fragIndex);
            ackManager.notifyAck(ackKey);
        } else {
            byte nextHop2 = findNextHopFor(finalDest);
            if (nextHop2 == 0) {
                System.out.println("No route to forward ACK for finalDest=" + finalDest + ". Using broadcast fallback.");
                nextHop2 = BROADCAST_ADDR;
            }
            sendAckFrame(srcAddr, finalDest, nextHop2, messageID, fragIndex);
        }
    }

    private void sendAckFrame(byte srcAddr, byte finalDest, byte nextHop,
                              short messageID, byte fragIndex) {
        ByteBuffer buf = buildFrameHeader(FLAG_ACK, srcAddr, finalDest, nextHop,
                                          messageID, fragIndex, (byte)1);
        buf.flip();
        macTxQueue.offer(new MacFrame(buf));
    }

    private void stopAndWaitSend(String message, byte finalDest) throws InterruptedException {
        byte[] dataBytes = message.getBytes(StandardCharsets.UTF_8);
        short msgID = (short)new Random().nextInt(Short.MAX_VALUE);
        int maxPayload = MAX_DATA_LEN - HEADER_LEN; // 24 bytes for payload
        int totalFrags = (int)Math.ceil(dataBytes.length / (double)maxPayload);
        byte nextHop;
        if (finalDest == BROADCAST_ADDR) {
            nextHop = BROADCAST_ADDR;
        } else {
            nextHop = findNextHopFor(finalDest);
            if (nextHop == 0) {
                System.out.println("No route to " + finalDest + " from " + localAddress);
                return;
            }
        }
        int offset = 0;
        final int MAX_RETRIES = 10;
        for (int fragIdx = 0; fragIdx < totalFrags; fragIdx++) {
            int length = Math.min(maxPayload, dataBytes.length - offset);
            byte[] fragment = Arrays.copyOfRange(dataBytes, offset, offset + length);
            offset += length;
            ByteBuffer frame = buildFrameHeader(FLAG_DATA, localAddress, finalDest, nextHop,
                                                msgID, (byte)fragIdx, (byte)totalFrags);
            frame.put(fragment);
            frame.flip();
            if (finalDest == BROADCAST_ADDR) {
                macTxQueue.offer(new MacFrame(frame));
                continue;
            }
            String ackKey = ackManager.makeAckKey(localAddress, msgID, (byte)fragIdx);
            boolean acked = false;
            int attempts = 0;
            while (!acked && attempts < MAX_RETRIES) {
                attempts++;
                macTxQueue.offer(new MacFrame(frame.duplicate()));
                acked = ackManager.waitForAck(ackKey, 15000);
                if (!acked) {
                    System.out.println("No ACK for frag " + fragIdx + ", attempt " + attempts);
                }
                else{
                    System.out.println("ACK for frag " + fragIdx + " received on attempt " +attempts);
                }
            }
            if (!acked) {
                System.out.println("Stop-and-Wait: giving up after " + MAX_RETRIES + " attempts.");
                return;
            }
        }
    }

    private void macLoop() {
        Random rand = new Random();
        while (true) {
            // Wait for a frame to send.
            MacFrame frame = null;
            try {
                frame = macTxQueue.take();
            } catch (InterruptedException e) {
                System.err.println("macLoop interrupted while taking frame: " + e);
                return;
            }

            // Wait until the channel is free.
            while (channelBusy) {
                try {
                    Thread.sleep(5);
                } catch (InterruptedException ex) {
                    return;
                }
            }

            // Wait a random backoff delay (to stagger simultaneous attempts).
            long backoffDelay = rand.nextInt(200);
            try {
                Thread.sleep(backoffDelay);
            } catch (InterruptedException e) {
                return;
            }

            // Send an INTENT frame to reserve the channel.
            sendIntentFrame();

            // Wait a short reservation period for the INTENT to propagate.
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                return;
            }

            // Check if during that reservation, the channel has been marked busy.
            if (channelBusy) {
                // Another node seems to be transmitting/reserved; requeue the frame and try later.
                macTxQueue.offer(frame);
                continue;
            }

            // Claim the channel.
            channelBusy = true;
            // Inform neighbors by sending a BUSY frame.
            sendBusyFrame();

            // Transmit the frame.
            try {
                sendQueue.put(new Message(MessageType.DATA, frame.frame));
            } catch (InterruptedException e) {
                System.err.println("Failed to transmit frame: " + e);
            }

            // Wait a short period to ensure the transmission completes.
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                return;
            }

            // Release the channel by sending a FREE frame.
            sendFreeFrame();
            // Mark channel as free locally.
            channelBusy = false;
        }
    }
    private Set<String> intentRelayCache = Collections.newSetFromMap(new ConcurrentHashMap<>());

    // Utility method to build a unique key for an INTENT frame:
    private String makeIntentKey(ByteBuffer buf) {
        // We assume the sender’s address is stored at position 1
        byte sender = buf.get(1);
        return "INTENT:" + sender;
    }

    private ByteBuffer buildIntentFrame() {
        ByteBuffer buf = ByteBuffer.allocate(HEADER_LEN);
        buf.put(FLAG_INTENT);
        buf.put(localAddress); // include our local address as the sender identifier
        buf.put((byte) 0);      // finalDest not used here—could be zero or BROADCAST_ADDR
        buf.put(BROADCAST_ADDR); // nextHop set to broadcast
        buf.putShort((short) 0); // messageID not relevant for INTENT
        buf.put((byte) 0);       // fragIndex
        buf.put((byte) 1);       // totalFrags, fixed to 1
        buf.flip();
        return buf;
    }

    private void sendIntentFrame() {
        ByteBuffer buf = buildIntentFrame();
        try {
            sendQueue.put(new Message(MessageType.INTENT, buf));
        } catch (InterruptedException e) {
            System.err.println("Failed to send INTENT frame: " + e);
        }
    }

    private void sendBusyFrame() {
        ByteBuffer buf = ByteBuffer.allocate(1);
        buf.put((byte) 0x02); // BUSY
        buf.flip();
        try {
            sendQueue.put(new Message(MessageType.BUSY, buf));
        } catch (InterruptedException e) {
            System.err.println("Failed to send BUSY frame: " + e);
        }
    }

    private void sendFreeFrame() {
        ByteBuffer buf = ByteBuffer.allocate(1);
        buf.put((byte) 0x01); // FREE
        buf.flip();
        try {
            sendQueue.put(new Message(MessageType.FREE, buf));
        } catch (InterruptedException e) {
            System.err.println("Failed to send FREE frame: " + e);
        }
    }

    private void helloLoop() {
        try {
            while (true) {
                if (isAddressAssigned) {
                    sendHello();
                }
                cleanupOldNeighbors();
                Thread.sleep(25000);
            }
        } catch (InterruptedException e) {
            // exit thread
        }
    }

    private void sendHello() {
        ByteBuffer buf = buildFrameHeader(FLAG_HELLO, localAddress,
                                          BROADCAST_ADDR, BROADCAST_ADDR,
                                          (short)0, (byte)0, (byte)1);
        buf.flip();
        macTxQueue.offer(new MacFrame(buf));
    }

    private void updateNeighbor(byte neighborAddr) {
        neighbors.put(neighborAddr, System.currentTimeMillis());
        if (callback != null) {
            callback.onNeighborsChanged(neighbors.keySet());
        }
    }

    private void cleanupOldNeighbors() {
        long now = System.currentTimeMillis();
        boolean changed = false;
        for (Map.Entry<Byte, Long> entry : neighbors.entrySet()) {
            if (now - entry.getValue() > NEIGHBOR_TIMEOUT_MS) {
                neighbors.remove(entry.getKey());
                changed = true;
            }
        }
        if (changed && callback != null) {
            callback.onNeighborsChanged(neighbors.keySet());
        }
    }

    private void handleAddrClaim(byte srcAddr, byte[] payload) {
        if (payload.length < 1) return;
        byte claimed = payload[0];
        if (isAddressAssigned && localAddress == claimed) {
            broadcastAddressConflict(claimed);
        } else {
            for (Byte n : neighbors.keySet()) {
                if (n == claimed) {
                    broadcastAddressConflict(claimed);
                    break;
                }
            }
        }
    }

    private void broadcastAddressConflict(byte claimed) {
        ByteBuffer buf = buildFrameHeader(FLAG_ADDR_CONFLICT, localAddress,
                                          BROADCAST_ADDR, BROADCAST_ADDR,
                                          (short)0, (byte)0, (byte)1);
        buf.put(claimed);
        buf.flip();
        macTxQueue.offer(new MacFrame(buf));
    }

    private void handleAddrConflict(byte[] payload) {
        if (payload.length < 1) return;
        byte claimed = payload[0];
        if (!isAddressAssigned) {
            synchronized (addrLock) {
                localConflictCandidate = claimed;
                hasConflict = true;
            }
        }
    }

    /**
     * Returns a string representing the current routing table.
     * Each entry is formatted as "Node X, nexthop = Y, distance = Z".
     * The entries are sorted by the destination node for consistency.
     */
    public String getRoutingTableString() {
        StringBuilder sb = new StringBuilder();
        List<Byte> keys = new ArrayList<>(routingTable.keySet());
        // Sort keys numerically for easy reading
        Collections.sort(keys, Comparator.comparingInt(b -> b));
        for (Byte dest : keys) {
            RouteInfo info = routingTable.get(dest);
            sb.append("Node ").append(dest)
                    .append(", nexthop = ").append(info.nextHop)
                    .append(", distance = ").append(info.distance)
                    .append("\n");
        }
        return sb.toString();
    }

    // Utility: Build Frame Header (8 bytes)
    /*
       [0] controlFlags
       [1] srcAddr
       [2] finalDest
       [3] nextHop
       [4..5] messageID
       [6] fragIndex
       [7] totalFrags
    */
    private ByteBuffer buildFrameHeader(byte controlFlags,
                                        byte srcAddr,
                                        byte finalDest,
                                        byte nextHop,
                                        short messageID,
                                        byte fragIndex,
                                        byte totalFrags) {
        ByteBuffer buf = ByteBuffer.allocate(MAX_DATA_LEN);
        buf.put(controlFlags);
        buf.put(srcAddr);
        buf.put(finalDest);
        buf.put(nextHop);
        buf.putShort(messageID);
        buf.put(fragIndex);
        buf.put(totalFrags);
        return buf;
    }
}