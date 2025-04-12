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
 * ProtocolLayer handles:
 *   - Dynamic Addressing (if localAddress==0)
 *   - MAC (naive random backoff)
 *   - Stop-and-Wait reliability (ACKs)
 *   - Basic multi-hop broadcast
 *   - Neighbor discovery
 */
public class ProtocolLayer {

    // ========== Constants ==========

    public static final byte BROADCAST_ADDR = (byte) 0xFF;

    // Byte-level frames from the emulator: 32 max
    private static final int MAX_DATA_LEN = 32;
    // We'll store a 7-byte (or more) header for normal data/ACK:
    private static final int HEADER_LEN = 7;

    // Control flags (previous + new)
    private static final byte FLAG_DATA        = 0x00;
    private static final byte FLAG_HELLO       = 0x01;
    private static final byte FLAG_ACK         = 0x02;
    private static final byte FLAG_ADDR_CLAIM  = 0x03; // new
    private static final byte FLAG_ADDR_CONFLICT = 0x04; // new

    // If we haven't assigned an address, we set isAddressAssigned = false.
    // Then we do dynamic addressing.
    private volatile boolean isAddressAssigned = false;

    // ========== Fields ==========

    private volatile byte localAddress;
    private BlockingQueue<Message> recvQueue; // from Client.Listener
    private BlockingQueue<Message> sendQueue; // to Client.Sender

    // The MAC queue
    private BlockingQueue<ByteBuffer> macTxQueue = new LinkedBlockingQueue<>();

    // Channel busy/free
    private volatile boolean channelBusy = false;

    // Neighbors: neighborAddr -> lastHeardTime
    private ConcurrentHashMap<Byte, Long> neighbors = new ConcurrentHashMap<>();
    private static final long NEIGHBOR_TIMEOUT_MS = 30000; // 30s

    // Reassembly storage
    private Map<String, MessageFragmentBuffer> reassemblyMap = new ConcurrentHashMap<>();
    private Set<String> seenFragmentIds = Collections.newSetFromMap(new ConcurrentHashMap<>());

    // For delivering messages to MyProtocol
    public interface ProtocolCallback {
        void onReceivedChatMessage(String fromAddr, String message);
        void onNeighborsChanged(Set<Byte> currentNeighbors);
    }
    private ProtocolCallback callback;

    // ACK manager for Stop-and-Wait
    private AckManager ackManager = new AckManager();

    // For dynamic addressing
    private final Object addrLock = new Object();
    private static final int ADDR_ASSIGN_TIMEOUT_MS = 3000; // Grace period to detect conflicts

    public ProtocolLayer(byte initialAddress,
                         BlockingQueue<Message> recvQueue,
                         BlockingQueue<Message> sendQueue,
                         ProtocolCallback callback)
    {
        this.localAddress = initialAddress;
        this.recvQueue = recvQueue;
        this.sendQueue = sendQueue;
        this.callback = callback;

        // If initialAddress != 0, we consider that assigned (static). Otherwise, dynamic.
        if (this.localAddress != 0) {
            isAddressAssigned = true;
        }
    }

    // ========== Lifecycle ==========

    public void start() {
        // 1) Start the thread that processes incoming frames
        new Thread(this::receiveLoop, "Protocol-ReceiveLoop").start();

        // 2) Start the MAC loop
        new Thread(this::macLoop, "Protocol-MacLoop").start();

        // 3) Possibly do dynamic addressing if localAddress == 0
        if (!isAddressAssigned) {
            new Thread(this::addressAssignmentLoop, "AddressAssignmentLoop").start();
        }

        // 4) Once we have an address, do normal HELLO
        new Thread(this::helloLoop, "Protocol-HelloLoop").start();
    }

    /**
     * For the application: get our final local address (once assigned).
     */
    public byte getLocalAddress() {
        return localAddress;
    }

    // ========== Public Chat API ==========

    /**
     * Send a user message (fragmented + Stop-and-Wait ACK)
     */
    public void sendChatMessage(String message, byte destAddr) {
        // If not assigned yet, we might want to block or reject. For a simple approach:
        if (!isAddressAssigned) {
            System.out.println("Address not assigned yet. Cannot send chat message.");
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

    /**
     * Returns an unmodifiable set of known neighbors
     */
    public Set<Byte> getNeighbors() {
        return Collections.unmodifiableSet(neighbors.keySet());
    }

    // ========== Private Methods ==========

    /**
     * If localAddress == 0, we repeatedly pick a random address, broadcast an ADDR_CLAIM,
     * wait to see if we get an ADDR_CONFLICT. If none, we finalize. If conflict, pick again.
     */
    private void addressAssignmentLoop() {
        Random rand = new Random();
        while (!isAddressAssigned) {
            // 1) pick a random address in [1..200], for instance
            byte candidate = (byte) (1 + rand.nextInt(200));

            // 2) broadcast ADDR_CLAIM with that candidate
            System.out.println("Attempting to claim address: " + candidate);
            broadcastAddressClaim(candidate);

            // 3) Wait ADDR_ASSIGN_TIMEOUT_MS for conflicts
            boolean conflict = false;
            long startTime = System.currentTimeMillis();
            while (System.currentTimeMillis() - startTime < ADDR_ASSIGN_TIMEOUT_MS) {
                // We'll sleep small intervals and check for conflicts
                if (localAddress != 0) {
                    // Means we somehow got assigned in the meantime
                    break;
                }
                synchronized (addrLock) {
                    // Did we mark a conflict? We'll see below
                    if (localConflictCandidate == candidate) {
                        conflict = true;
                        break;
                    }
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    return; // stop
                }
            }

            if (!conflict) {
                // We assume success
                synchronized (addrLock) {
                    if (!hasConflict) {
                        localAddress = candidate;
                        isAddressAssigned = true;
                        System.out.println("Successfully assigned address: " + localAddress);
                    } else {
                        // If we discovered conflict after all
                        System.out.println("Conflict found, retrying...");
                        hasConflict = false;
                        localConflictCandidate = 0;
                    }
                }
            } else {
                // conflict = true
                synchronized (addrLock) {
                    System.out.println("Conflict detected for address: " + candidate + ". Retrying...");
                    hasConflict = false;
                    localConflictCandidate = 0;
                }
            }
        }
    }

    // We'll store some simple conflict signals here
    private boolean hasConflict = false;
    private byte localConflictCandidate = 0;

    private void broadcastAddressClaim(byte candidate) {
        ByteBuffer buf = buildFrameHeader(
                FLAG_ADDR_CLAIM,
                (byte)0, // 0 means "unassigned" from me
                BROADCAST_ADDR,
                (short) 0,  // messageID
                (byte) 0,   // fragIdx
                (byte) 1    // totalFrags
        );
        // We'll put 1 byte of "candidate" in the payload
        buf.put(candidate);
        buf.flip();
        macTxQueue.offer(buf);
    }

    /**
     * The main receiving loop
     */
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
                        // Could track progress if desired
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

    /**
     * Parse incoming frames and handle them
     */
    private void handleIncomingData(ByteBuffer buf) {
        buf.rewind();
        byte[] raw = new byte[buf.remaining()];
        buf.get(raw);

        if (raw.length < HEADER_LEN) {
            // too short
            return;
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

        // If srcAddr != 0, that means the sender has an assigned address. Update neighbor info
        if (srcAddr != 0) {
            updateNeighbor(srcAddr);
        }

        // Dispatch by controlFlags
        switch (controlFlags) {
            case FLAG_HELLO:
                // we do nothing else, neighbor updated
                return;

            case FLAG_ACK:
                // It's an ACK for some data fragment
                String ackKey = ackManager.makeAckKey(dstAddr, messageID, fragmentIndex);
                ackManager.notifyAck(ackKey);
                return;

            case FLAG_ADDR_CLAIM:
                handleAddrClaim(srcAddr, payload);
                return;

            case FLAG_ADDR_CONFLICT:
                handleAddrConflict(payload);
                return;

            default:
                // It's normal data or something else
                handleNormalData(controlFlags, srcAddr, dstAddr, messageID, fragmentIndex, totalFrags, payload);
                break;
        }
    }

    /**
     * Handle receiving an address claim from an unassigned node.
     * "payload[0]" is the candidate address they want.
     */
    private void handleAddrClaim(byte srcAddr, byte[] payload) {
        if (payload.length < 1) return;
        byte claimed = payload[0];

        // If we are using that address ourselves, or we have a known neighbor using that address, conflict.
        if (isAddressAssigned && localAddress == claimed) {
            // We broadcast conflict
            broadcastAddressConflict(claimed);
        } else {
            // Check neighbors
            for (Byte n : neighbors.keySet()) {
                if (n == claimed) {
                    broadcastAddressConflict(claimed);
                    break;
                }
            }
        }
    }

    /**
     * Broadcast address conflict for a specific address
     */
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

    /**
     * If we are in the middle of claiming an address, seeing a conflict sets a flag.
     */
    private void handleAddrConflict(byte[] payload) {
        if (payload.length < 1) return;
        byte claimed = payload[0];

        // If we are not assigned yet, and we're attempting that address => conflict
        if (!isAddressAssigned) {
            synchronized (addrLock) {
                localConflictCandidate = claimed;
                hasConflict = true;
            }
        }
    }

    private void handleNormalData(byte controlFlags, byte srcAddr, byte dstAddr,
                                  short messageID, byte fragIndex, byte totalFrags,
                                  byte[] payload)
    {
        // Flood / duplication check
        String fragKey = srcAddr + ":" + messageID + ":" + fragIndex;
        if (seenFragmentIds.contains(fragKey)) {
            return;
        }
        seenFragmentIds.add(fragKey);

        // Basic broadcast forward
        if (dstAddr == BROADCAST_ADDR && srcAddr != localAddress) {
            forwardBroadcast(controlFlags, srcAddr, dstAddr, messageID, fragIndex, totalFrags, payload);
        }

        // If we are the final dest or broadcast, reassemble & deliver
        if (dstAddr == localAddress || dstAddr == BROADCAST_ADDR) {
            reassembleAndDeliver(srcAddr, messageID, fragIndex, totalFrags, payload);
            // send an ACK
            if (controlFlags == FLAG_DATA) {
                sendAckTo(srcAddr, messageID, fragIndex);
            }
        }
    }

    /**
     * Forward broadcast frames
     */
    private void forwardBroadcast(byte controlFlags, byte srcAddr, byte dstAddr,
                                  short msgID, byte fragIdx, byte totalFrags, byte[] payload)
    {
        ByteBuffer frame = buildFrameHeader(controlFlags, srcAddr, dstAddr, msgID, fragIdx, totalFrags);
        frame.put(payload);
        frame.flip();
        macTxQueue.offer(frame);
    }

    /**
     * Reassemble + deliver
     */
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

    /**
     * Send a data fragment with Stop-and-Wait reliability
     */
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

            // ack key
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
                return; // stop sending further fragments
            }
        }
    }

    /**
     * Send an ACK
     */
    private void sendAckTo(byte destAddr, short messageID, byte fragIndex) {
        ByteBuffer ackFrame = buildFrameHeader(
                FLAG_ACK,
                localAddress,
                destAddr,
                messageID,
                fragIndex,
                (byte)1  // totalFrags=1 for an ACK
        );
        ackFrame.flip();
        macTxQueue.offer(ackFrame);
    }

    /**
     * Build a frame with the 7-byte header
     */
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

    /**
     * MAC loop: wait for free, random backoff, etc.
     */
    private void macLoop() {
        Random rand = new Random();
        while (true) {
            try {
                ByteBuffer frame = macTxQueue.take();

                while (channelBusy) {
                    Thread.sleep(10);
                }
                // small random backoff
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

    /**
     * Periodic Hello + neighbor cleanup
     * But we only do HELLO if we have an assigned address
     */
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
}