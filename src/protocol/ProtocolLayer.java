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
import protocol.MessageFragmentBuffer;

/**
 * ProtocolLayer with:
 *  1) Dynamic Addressing (if localAddress=0)
 *  2) MAC with naive random backoff
 *  3) Stop-and-Wait reliability
 *  4) Distance Vector routing for multi-hop unicast
 *  5) Basic broadcast/flood
 */
public class ProtocolLayer {

    // ================= Constants =================

    public static final byte BROADCAST_ADDR = (byte) 0xFF;

    // Frame sizes
    private static final int MAX_DATA_LEN = 32;
    private static final int HEADER_LEN   = 7;

    // Control flags
    private static final byte FLAG_DATA          = 0x00;
    private static final byte FLAG_HELLO         = 0x01;
    private static final byte FLAG_ACK           = 0x02;
    private static final byte FLAG_ADDR_CLAIM    = 0x03;
    private static final byte FLAG_ADDR_CONFLICT = 0x04;
    // New for Distance Vector
    private static final byte FLAG_DV            = 0x05;

    // If initialAddress != 0, we skip dynamic assignment
    private volatile boolean isAddressAssigned = false;

    // ================= Fields =================

    private volatile byte localAddress;
    private BlockingQueue<Message> recvQueue;
    private BlockingQueue<Message> sendQueue;

    // The "MAC TX" queue
    private BlockingQueue<ByteBuffer> macTxQueue = new LinkedBlockingQueue<>();

    // Channel busy/free
    private volatile boolean channelBusy = false;

    // Neighbors
    private ConcurrentHashMap<Byte, Long> neighbors = new ConcurrentHashMap<>();
    private static final long NEIGHBOR_TIMEOUT_MS = 30000; // 30s

    // Reassembly for multi-fragment messages
    private Map<String, MessageFragmentBuffer> reassemblyMap = new ConcurrentHashMap<>();
    // For ignoring duplicate fragments
    private Set<String> seenFragmentIds = Collections.newSetFromMap(new ConcurrentHashMap<>());

    // Callback for delivering user messages up to the app
    public interface ProtocolCallback {
        void onReceivedChatMessage(String fromAddr, String message);
        void onNeighborsChanged(Set<Byte> currentNeighbors);
    }
    private ProtocolCallback callback;

    // Stop-and-Wait ACK manager
    private AckManager ackManager = new AckManager();

    // For dynamic addressing
    private final Object addrLock = new Object();
    private boolean hasConflict = false;
    private byte localConflictCandidate = 0;
    private static final int ADDR_ASSIGN_TIMEOUT_MS = 3000;

    // =============== Distance Vector ===============
    // We'll store a table: destination -> (distance, nextHop)
    // Distance is a simple hop count. nextHop = immediate neighbor used.
    // Infinity = 999
    private static final int INFINITY = 999;
    private Map<Byte, RouteInfo> routingTable = new ConcurrentHashMap<>();

    // We'll broadcast DV every 10s, or when changes occur. For simplicity, let's do periodic.
    private static final long DV_BROADCAST_INTERVAL_MS = 10000;

    private static class RouteInfo {
        public int distance;   // hop count
        public byte nextHop;
        public RouteInfo(int distance, byte nextHop) {
            this.distance = distance;
            this.nextHop = nextHop;
        }
    }

    public ProtocolLayer(byte initialAddress,
                         BlockingQueue<Message> recvQueue,
                         BlockingQueue<Message> sendQueue,
                         ProtocolCallback callback)
    {
        this.localAddress = initialAddress;
        this.recvQueue = recvQueue;
        this.sendQueue = sendQueue;
        this.callback = callback;

        if (this.localAddress != 0) {
            isAddressAssigned = true;
            // Initialize routing table to reflect that we know ourselves at dist=0
            routingTable.put(localAddress, new RouteInfo(0, localAddress));
        }
    }

    // ============= Lifecycle Methods =============

    public void start() {
        // Main receiving thread
        new Thread(this::receiveLoop, "Protocol-ReceiveLoop").start();

        // MAC loop
        new Thread(this::macLoop, "Protocol-MacLoop").start();

        // Dynamic addressing if needed
        if (!isAddressAssigned) {
            new Thread(this::addressAssignmentLoop, "AddressAssignmentLoop").start();
        }

        // HELLO + neighbor cleanup
        new Thread(this::helloLoop, "Protocol-HelloLoop").start();

        // Distance Vector broadcast loop
        new Thread(this::dvLoop, "Protocol-DVLoop").start();
    }

    public byte getLocalAddress() {
        return localAddress;
    }

    /**
     * For the application: send a chat message (Stop-and-Wait) to a unicast or broadcast.
     */
    public void sendChatMessage(String message, byte destAddr) {
        if (!isAddressAssigned) {
            System.out.println("Address not assigned yet, cannot send messages.");
            return;
        }
        new Thread(() -> {
            try {
                stopAndWaitSend(message, destAddr);
            } catch (InterruptedException e) {
                System.err.println("sendChatMessage interrupted: " + e);
            }
        }, "StopAndWaitSender").start();
    }

    public Set<Byte> getNeighbors() {
        return Collections.unmodifiableSet(neighbors.keySet());
    }

    // ============= Dynamic Addressing =============

    private void addressAssignmentLoop() {
        Random rand = new Random();
        while (!isAddressAssigned) {
            // Pick a random address in [1..200]
            byte candidate = (byte) (1 + rand.nextInt(200));
            System.out.println("Attempting to claim address: " + candidate);
            broadcastAddressClaim(candidate);

            // Wait for conflicts
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
                // finalize
                synchronized (addrLock) {
                    if (!hasConflict) {
                        localAddress = candidate;
                        isAddressAssigned = true;
                        System.out.println("Assigned address: " + localAddress);
                        // Setup routingTable for ourselves
                        routingTable.put(localAddress, new RouteInfo(0, localAddress));
                    } else {
                        System.out.println("Conflict found after all, retrying...");
                        hasConflict = false;
                        localConflictCandidate = 0;
                    }
                }
            } else {
                synchronized (addrLock) {
                    System.out.println("Conflict detected for address " + candidate + ", retrying...");
                    hasConflict = false;
                    localConflictCandidate = 0;
                }
            }
        }
    }

    private void broadcastAddressClaim(byte candidate) {
        ByteBuffer buf = buildFrameHeader(
                FLAG_ADDR_CLAIM,
                (byte)0, // src=0 means unassigned
                BROADCAST_ADDR,
                (short)0, (byte)0, (byte)1
        );
        buf.put(candidate);
        buf.flip();
        macTxQueue.offer(buf);
    }

    // ============= Distance Vector =============

    /**
     * Periodically broadcast our routing table to neighbors.
     */
    private void dvLoop() {
        try {
            while (true) {
                Thread.sleep(DV_BROADCAST_INTERVAL_MS);
                if (isAddressAssigned) {
                    broadcastDistanceVector();
                }
            }
        } catch (InterruptedException e) {
            // end
        }
    }

    /**
     * Send our routing table in a broadcast frame with FLAG_DV.
     * Payload format: [countEntries, (addr, distance), (addr, distance), ...]
     * We'll store distance as a single byte if < 255, otherwise 255 for "infinity".
     */
    private void broadcastDistanceVector() {
        // Build the table in a byte array
        List<Byte> payloadList = new ArrayList<>();
        // We'll skip ourselves or we can include ourselves. Let's include everything.
        Set<Byte> destinations = routingTable.keySet();

        // max possible entries is 255 if there's a huge network
        int count = destinations.size();
        payloadList.add((byte) count);

        for (Byte dest : destinations) {
            RouteInfo ri = routingTable.get(dest);
            payloadList.add(dest);
            // clamp distance to 255 if > 255
            int dist = (ri.distance > 255) ? 255 : ri.distance;
            payloadList.add((byte) dist);
        }

        // Convert to array
        byte[] payload = new byte[payloadList.size()];
        for (int i = 0; i < payloadList.size(); i++) {
            payload[i] = payloadList.get(i);
        }

        // Build the frame
        ByteBuffer frame = buildFrameHeader(FLAG_DV, localAddress, BROADCAST_ADDR,
                                            (short)0, (byte)0, (byte)1);
        frame.put(payload);
        frame.flip();
        macTxQueue.offer(frame);
    }

    /**
     * Parse a DV frame from neighbor srcAddr, update local table using Bellman-Ford logic:
     *  dist(me->X) > dist(me->srcAddr) + dist(srcAddr->X)
     */
    private void handleDistanceVector(byte srcAddr, byte[] payload) {
        // parse
        if (payload.length < 1) return;
        int count = payload[0] & 0xFF;
        int idx = 1;
        // Ensure we have the correct length
        if (payload.length < 1 + count*2) {
            System.out.println("DV parse error: not enough bytes");
            return;
        }

        // distance to neighbor srcAddr is presumably 1 hop, or maybe we track direct neighbors as 1
        int distToNeighbor = getDistanceTo(srcAddr);
        if (distToNeighbor == INFINITY) {
            // We don't have a direct route to srcAddr yet? We'll consider it 1 hop if it's in neighbors
            if (neighbors.containsKey(srcAddr)) {
                distToNeighbor = 1;
                // Insert or update local route to srcAddr
                updateRoute(srcAddr, 1, srcAddr);
            } else {
                // We got a DV from a node we don't think is reachable. Possibly a multi-hop scenario?
                // We'll guess it's dist=1 if we see it directly.
                distToNeighbor = 1;
                updateRoute(srcAddr, 1, srcAddr);
            }
        }

        for (int i = 0; i < count; i++) {
            byte dest = payload[idx++];
            int theirDist = (payload[idx++] & 0xFF);
            // Bellman-Ford: potential distance = dist(me->srcAddr) + theirDist
            int newDist = distToNeighbor + theirDist;
            // If newDist < dist(me->dest), update
            int oldDist = getDistanceTo(dest);
            if (newDist < oldDist) {
                // update route
                updateRoute(dest, newDist, srcAddr);
            }
        }
    }

    /**
     * Get the current known distance to some destination. If not known, return INFINITY.
     */
    private int getDistanceTo(byte dest) {
        RouteInfo ri = routingTable.get(dest);
        if (ri == null) return INFINITY;
        return ri.distance;
    }

    /**
     * Update or insert route in our table. Then we might want to re-broadcast.
     * We'll just do a quick update for now. We might do a triggered DV broadcast if desired.
     */
    private void updateRoute(byte dest, int dist, byte nextHop) {
        routingTable.put(dest, new RouteInfo(dist, nextHop));
        // Optionally: print debug
        System.out.println("Node " + localAddress + " updated route to " + dest + ": dist=" + dist + ", nextHop=" + nextHop);
    }

    // ============= Receiving Loop =============

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
                    case DATA:
                    case DATA_SHORT:
                        handleIncomingData(msg.getData());
                        break;
                    case SENDING:
                    case DONE_SENDING:
                        // track if needed
                        break;
                    default:
                        // token accepted, token rejected, etc.
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

        if (raw.length < HEADER_LEN) {
            return; // too short
        }

        byte controlFlags   = raw[0];
        byte srcAddr        = raw[1];
        byte dstAddr        = raw[2];
        short messageID     = (short) (((raw[3] & 0xFF) << 8) | (raw[4] & 0xFF));
        byte fragmentIndex  = raw[5];
        byte totalFrags     = raw[6];

        int payloadLen = raw.length - HEADER_LEN;
        byte[] payload = new byte[payloadLen];
        System.arraycopy(raw, HEADER_LEN, payload, 0, payloadLen);

        // If srcAddr != 0, update neighbor
        if (srcAddr != 0) {
            updateNeighbor(srcAddr);
            // Also ensure route to srcAddr is at most 1 hop
            if (getDistanceTo(srcAddr) > 1) {
                updateRoute(srcAddr, 1, srcAddr);
            }
        }

        switch (controlFlags) {
            case FLAG_HELLO:
                // done
                return;
            case FLAG_ACK:
                // It's an ACK for a unicast. Let the ackManager know
                String ackKey = ackManager.makeAckKey(dstAddr, messageID, fragmentIndex);
                ackManager.notifyAck(ackKey);
                return;
            case FLAG_ADDR_CLAIM:
                handleAddrClaim(srcAddr, payload);
                return;
            case FLAG_ADDR_CONFLICT:
                handleAddrConflict(payload);
                return;
            case FLAG_DV:
                // This is a distance vector from srcAddr
                handleDistanceVector(srcAddr, payload);
                return;
            default:
                // It's normal data
                handleNormalData(controlFlags, srcAddr, dstAddr, messageID, fragmentIndex, totalFrags, payload);
        }
    }

    // ============= Normal Data (Unicast or Broadcast) =============

    private void handleNormalData(byte controlFlags, byte srcAddr, byte dstAddr,
                                  short messageID, byte fragIndex, byte totalFrags,
                                  byte[] payload)
    {
        String fragKey = srcAddr + ":" + messageID + ":" + fragIndex;
        if (seenFragmentIds.contains(fragKey)) {
            return;
        }
        seenFragmentIds.add(fragKey);

        if (dstAddr == BROADCAST_ADDR && srcAddr != localAddress) {
            // forward broadcast
            forwardBroadcast(controlFlags, srcAddr, dstAddr, messageID, fragIndex, totalFrags, payload);
        }
        else if (dstAddr != localAddress && dstAddr != BROADCAST_ADDR) {
            // unicast, but we're not final. we should forward if we have a route
            if (routingTable.containsKey(dstAddr)) {
                byte nextHop = routingTable.get(dstAddr).nextHop;
                if (nextHop == localAddress) {
                    // Strange: means we think we are the best route to get to ourselves?
                    // Possibly a routing table confusion. We'll discard.
                    System.out.println("Routing loop or invalid route for " + dstAddr + " - discarding.");
                    return;
                }
                // forward unicast to nextHop
                forwardUnicast(controlFlags, srcAddr, dstAddr, messageID, fragIndex, totalFrags, payload, nextHop);
            } else {
                System.out.println("No route to " + dstAddr + " from " + localAddress + ", discarding.");
            }
            return;
        }

        // If we are final or broadcast
        reassembleAndDeliver(srcAddr, messageID, fragIndex, totalFrags, payload);
        // If it's normal data, send ACK (Stop-and-Wait)
        if (controlFlags == FLAG_DATA && dstAddr != BROADCAST_ADDR) {
            sendAckTo(srcAddr, messageID, fragIndex);
        }
    }

    private void forwardBroadcast(byte controlFlags, byte srcAddr, byte dstAddr,
                                  short messageID, byte fragIndex, byte totalFrags,
                                  byte[] payload)
    {
        ByteBuffer frame = buildFrameHeader(controlFlags, srcAddr, dstAddr, messageID, fragIndex, totalFrags);
        frame.put(payload);
        frame.flip();
        macTxQueue.offer(frame);
    }

    private void forwardUnicast(byte controlFlags, byte srcAddr, byte dstAddr,
                                short messageID, byte fragIdx, byte totalFrags,
                                byte[] payload, byte nextHop)
    {
        ByteBuffer frame = buildFrameHeader(controlFlags, srcAddr, dstAddr, messageID, fragIdx, totalFrags);
        frame.put(payload);
        frame.flip();
        macTxQueue.offer(frame);
    }

    private void reassembleAndDeliver(byte srcAddr, short messageID,
                                      byte fragIndex, byte totalFrags, byte[] payload)
    {
        String mapKey = srcAddr + ":" + messageID;
        MessageFragmentBuffer buf = reassemblyMap.get(mapKey);
        if (buf == null) {
            buf = new MessageFragmentBuffer(totalFrags);
            reassemblyMap.put(mapKey, buf);
        }
        buf.addFragment(fragIndex, payload);

        if (buf.isComplete()) {
            byte[] fullData = buf.reassemble();
            String msgText = new String(fullData, StandardCharsets.UTF_8);
            if (callback != null) {
                callback.onReceivedChatMessage("Node " + srcAddr, msgText);
            }
            reassemblyMap.remove(mapKey);
        }
    }

    // ============= Stop-and-Wait Send =============

    private void stopAndWaitSend(String message, byte destAddr) throws InterruptedException {
        byte[] dataBytes = message.getBytes(StandardCharsets.UTF_8);
        short msgID = (short) new Random().nextInt(Short.MAX_VALUE);

        int maxPayload = MAX_DATA_LEN - HEADER_LEN;
        int totalFrags = (int) Math.ceil(dataBytes.length / (double) maxPayload);
        int offset = 0;

        final int MAX_RETRIES = 5;
        for (int fragIdx = 0; fragIdx < totalFrags; fragIdx++) {
            int length = Math.min(maxPayload, dataBytes.length - offset);
            byte[] fragment = Arrays.copyOfRange(dataBytes, offset, offset + length);
            offset += length;

            ByteBuffer frame = buildFrameHeader(FLAG_DATA, localAddress, destAddr, msgID,
                                                (byte)fragIdx, (byte)totalFrags);
            frame.put(fragment);
            frame.flip();

            String ackKey = ackManager.makeAckKey(localAddress, msgID, (byte)fragIdx);

            boolean acked = false;
            int attempts = 0;
            while (!acked && attempts < MAX_RETRIES) {
                attempts++;
                macTxQueue.offer(frame.duplicate());
                acked = ackManager.waitForAck(ackKey, 2000); // 2s
                if (!acked) {
                    System.out.println("No ACK for frag " + fragIdx + ", attempt " + attempts);
                }
            }
            if (!acked) {
                System.out.println("Stop-and-Wait: giving up after " + MAX_RETRIES + " attempts.");
                return;
            }
        }
    }

    private void sendAckTo(byte destAddr, short messageID, byte fragIndex) {
        ByteBuffer ackFrame = buildFrameHeader(
                FLAG_ACK,
                localAddress,
                destAddr,
                messageID,
                fragIndex,
                (byte)1
        );
        ackFrame.flip();
        macTxQueue.offer(ackFrame);
    }

    // ============= Address Conflict Handling =============

    private void handleAddrClaim(byte srcAddr, byte[] payload) {
        if (payload.length < 1) return;
        byte claimed = payload[0];
        if (isAddressAssigned && localAddress == claimed) {
            broadcastAddressConflict(claimed);
        } else {
            // check neighbors
            for (Byte n : neighbors.keySet()) {
                if (n == claimed) {
                    broadcastAddressConflict(claimed);
                    break;
                }
            }
        }
    }

    private void broadcastAddressConflict(byte claimed) {
        ByteBuffer buf = buildFrameHeader(
                FLAG_ADDR_CONFLICT,
                localAddress,
                BROADCAST_ADDR,
                (short)0, (byte)0, (byte)1
        );
        buf.put(claimed);
        buf.flip();
        macTxQueue.offer(buf);
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

    // ============= MAC Loop =============

    private void macLoop() {
        Random rand = new Random();
        while (true) {
            try {
                ByteBuffer frame = macTxQueue.take();
                while (channelBusy) {
                    Thread.sleep(10);
                }
                Thread.sleep(10 + rand.nextInt(20));
                if (channelBusy) {
                    macTxQueue.offer(frame);
                    continue;
                }
                sendQueue.put(new Message(MessageType.DATA, frame));
            } catch (InterruptedException e) {
                System.err.println("macLoop interrupted: " + e);
                return;
            }
        }
    }

    // ============= HELLO + Neighbor Cleanup =============

    private void helloLoop() {
        try {
            while (true) {
                if (isAddressAssigned) {
                    sendHello();
                }
                cleanupOldNeighbors();
                Thread.sleep(5000);
            }
        } catch (InterruptedException e) {
            // end
        }
    }

    private void sendHello() {
        ByteBuffer buf = buildFrameHeader(
                FLAG_HELLO,
                localAddress,
                BROADCAST_ADDR,
                (short)0, (byte)0, (byte)1
        );
        buf.flip();
        macTxQueue.offer(buf);
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
        for (Map.Entry<Byte, Long> e : neighbors.entrySet()) {
            if (now - e.getValue() > NEIGHBOR_TIMEOUT_MS) {
                neighbors.remove(e.getKey());
                changed = true;
            }
        }
        if (changed && callback != null) {
            callback.onNeighborsChanged(neighbors.keySet());
        }
    }

    // ============= Utility =============

    private ByteBuffer buildFrameHeader(byte controlFlags, byte srcAddr, byte dstAddr,
                                        short messageID, byte fragmentIndex, byte totalFrags)
    {
        ByteBuffer buf = ByteBuffer.allocate(MAX_DATA_LEN);
        buf.put(controlFlags);
        buf.put(srcAddr);
        buf.put(dstAddr);
        buf.putShort(messageID);
        buf.put(fragmentIndex);
        buf.put(totalFrags);
        return buf;
    }
}