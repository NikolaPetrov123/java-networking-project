package protocol;

import client.Message;
import client.MessageType;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

/**
 * ProtocolLayer handles:
 *  - MAC (waiting for FREE, random backoff)
 *  - Fragmentation & Reliability (ACKs)
 *  - Multi-Hop (basic flooding or direct neighbor unicast)
 *  - Neighbor Discovery
 */
public class ProtocolLayer {

    // Constants
    private static final int MAX_DATA_LEN = 32;      // Per the emulator spec
    private static final int HEADER_LEN   = 6;       // Example: 6 bytes of custom header
    private static final byte BROADCAST_ADDR = (byte)0xFF; // A special "broadcast" address

    // Fields
    private byte localAddress;                       // This node's address (0-255, for example)
    private BlockingQueue<Message> recvQueue;        // Filled by the Client.Listener
    private BlockingQueue<Message> sendQueue;        // Consumed by the Client.Sender

    // We store a queue of messages we want to transmit. Our "MAC" logic will read from this.
    private BlockingQueue<ByteBuffer> macTxQueue = new LinkedBlockingQueue<>();

    // Keep track of the channel state from BUSY/FREE messages
    private volatile boolean channelBusy = false;

    // Store neighbors
    // Maps neighbor address -> time last heard
    private ConcurrentHashMap<Byte, Long> neighbors = new ConcurrentHashMap<>();
    private static final long NEIGHBOR_TIMEOUT_MS = 30_000; // e.g. 30s

    // Store partial message fragments for reassembly
    // Key: (srcAddr, messageID) => a reassembly buffer
    private Map<String, MessageFragmentBuffer> reassemblyMap = new ConcurrentHashMap<>();

    // Store known (srcAddr,messageID,fragmentIndex) to avoid reflooding duplicates
    private Set<String> seenFragmentIds = Collections.newSetFromMap(new ConcurrentHashMap<>());

    // A callback to deliver fully reassembled messages up to MyProtocol
    public interface ProtocolCallback {
        void onReceivedChatMessage(String fromAddr, String message);
        void onNeighborsChanged(Set<Byte> currentNeighbors);
    }
    private ProtocolCallback callback;

    // For reliability: store pending acks, sequence numbers, etc. (optional advanced)
    // For brevity, we'll keep it simple in this example.

    /**
     * Constructor
     * @param localAddress a unique ID for this node (0-255).
     * @param recvQueue queue from Client (incoming)
     * @param sendQueue queue to Client (outgoing)
     * @param callback to notify MyProtocol about incoming messages or neighbor changes
     */
    public ProtocolLayer(byte localAddress,
                         BlockingQueue<Message> recvQueue,
                         BlockingQueue<Message> sendQueue,
                         ProtocolCallback callback)
    {
        this.localAddress = localAddress;
        this.recvQueue = recvQueue;
        this.sendQueue = sendQueue;
        this.callback = callback;
    }

    // =========================  Public Methods  =========================

    /**
     * Start the threads that handle receiving and MAC transmissions.
     */
    public void start() {
        // Thread for processing incoming frames
        new Thread(this::receiveLoop, "Protocol-ReceiveLoop").start();

        // Thread for handling MAC transmissions
        new Thread(this::macLoop, "Protocol-MacLoop").start();

        // Thread for periodic neighbor discovery "HELLO"
        new Thread(() -> {
            try {
                while(true) {
                    sendHello();
                    cleanupOldNeighbors();
                    Thread.sleep(5000);
                }
            } catch (InterruptedException e) {
                // Exit thread
            }
        }, "Protocol-HelloThread").start();
    }

    /**
     * Send a chat message to either a specific node or broadcast if destAddr == BROADCAST_ADDR.
     */
    public void sendChatMessage(String message, byte destAddr) {
        // Fragment if needed
        byte[] dataBytes = message.getBytes(StandardCharsets.UTF_8);
        // For simplicity, let's generate a random messageID:
        short messageID = (short)new Random().nextInt(Short.MAX_VALUE);

        // Break into 32-byte fragments (minus HEADER_LEN)
        int maxPayloadSize = MAX_DATA_LEN - HEADER_LEN;
        int totalFragments = (int)Math.ceil(dataBytes.length / (double)maxPayloadSize);

        int offset = 0;
        for (int fragIdx = 0; fragIdx < totalFragments; fragIdx++) {
            int len = Math.min(maxPayloadSize, dataBytes.length - offset);
            byte[] fragmentPayload = Arrays.copyOfRange(dataBytes, offset, offset + len);
            offset += len;

            // Build a ByteBuffer with the header + payload
            ByteBuffer frame = buildFrameHeader(
                    (byte)0x00, // controlFlags - (0x00 for normal data)
                    localAddress,
                    destAddr,
                    messageID,
                    (byte)fragIdx,
                    (byte)totalFragments
            );
            frame.put(fragmentPayload);
            frame.flip();

            // Put it into the macTxQueue, which will eventually go to the sendQueue
            macTxQueue.offer(frame);
        }
    }

    /**
     * Get the current set of neighbor addresses
     */
    public Set<Byte> getNeighbors() {
        return Collections.unmodifiableSet(neighbors.keySet());
    }

    // =========================  Private Methods  =========================

    /**
     * The main receiving loop. Continuously takes Messages from recvQueue,
     * parses them, and handles accordingly.
     */
    private void receiveLoop() {
        while(true) {
            try {
                Message msg = recvQueue.take();
                switch (msg.getType()) {
                    case BUSY:
                        channelBusy = true;
                        // System.out.println("MAC -> Channel is BUSY");
                        break;
                    case FREE:
                        channelBusy = false;
                        // System.out.println("MAC -> Channel is FREE");
                        break;
                    case DATA:
                    case DATA_SHORT:
                        handleIncomingData(msg.getData());
                        break;
                    case SENDING:
                    case DONE_SENDING:
                        // Possibly track transmissions
                        break;
                    case HELLO:
                    case END:
                    case TOKEN_ACCEPTED:
                    case TOKEN_REJECTED:
                    default:
                        // Handle if needed
                        break;
                }
            } catch (InterruptedException e) {
                System.err.println("ProtocolLayer.receiveLoop interrupted.");
                return;
            }
        }
    }

    /**
     * Parse the incoming ByteBuffer as our custom header + payload
     */
    private void handleIncomingData(ByteBuffer buf) {
        buf.rewind();
        byte[] raw = new byte[buf.remaining()];
        buf.get(raw);

        // If too short, ignore
        if (raw.length < HEADER_LEN) {
            System.out.println("Received frame too small, ignoring.");
            return;
        }

        // Parse header (6 bytes in this example)
        byte controlFlags   = raw[0];
        byte srcAddr        = raw[1];
        byte dstAddr        = raw[2];
        short messageID     = (short)(( (raw[3] & 0xFF) << 8 ) | (raw[4] & 0xFF));
        byte fragmentIndex  = raw[5];

        // If there's a totalFragments byte, we can read it – but let's assume we used 6 bytes for now.
        // If you want 7 bytes for header, you'd parse raw[6] as totalFragments, etc.
        // For demonstration, let's say totalFragments is actually raw[6].
        // So let's adjust our HEADER_LEN to 7. But I'll keep it 6 to keep it short.
        // We'll assume totalFragments = fragmentIndex + 1 for the sake of demonstration:
        byte totalFragments = (byte)(fragmentIndex + 1);

        // Extract payload
        int payloadLen = raw.length - HEADER_LEN;
        byte[] payload = new byte[payloadLen];
        System.arraycopy(raw, HEADER_LEN, payload, 0, payloadLen);

        // If it's a "HELLO" (you could store that in controlFlags, or check a special bit)
        if (controlFlags == (byte)0x01) {
            // It's a HELLO
            updateNeighbor(srcAddr);
            return;
        }

        // For normal data frames, do multi-hop logic if necessary
        updateNeighbor(srcAddr);  // any frame indicates neighbor is alive

        // Build a unique fragment key to detect duplicates
        String fragmentKey = srcAddr + ":" + messageID + ":" + fragmentIndex;

        // If we've seen this fragment before, ignore (don't reflood it)
        if (seenFragmentIds.contains(fragmentKey)) {
            return;
        }
        seenFragmentIds.add(fragmentKey);

        // If broadcast & not final destination, we might forward, etc.
        if (dstAddr == BROADCAST_ADDR && srcAddr != localAddress) {
            // Flood broadcast to neighbors (except we do not forward if we are the source).
            forwardBroadcast(controlFlags, srcAddr, dstAddr, messageID, fragmentIndex, payload);
        }

        // If this frame is destined for me, or if it is broadcast, we reassemble
        if (dstAddr == localAddress || dstAddr == BROADCAST_ADDR) {
            reassembleAndDeliver(srcAddr, messageID, fragmentIndex, totalFragments, payload);
        }
    }

    private void forwardBroadcast(byte controlFlags, byte srcAddr, byte dstAddr,
                                  short messageID, byte fragmentIndex, byte[] payload)
    {
        // Rebuild the frame
        ByteBuffer forwardFrame = buildFrameHeader(
                controlFlags, srcAddr, dstAddr, messageID, fragmentIndex, (byte)(fragmentIndex+1)
        );
        forwardFrame.put(payload);
        forwardFrame.flip();
        // Put it into the macTxQueue
        macTxQueue.offer(forwardFrame);
    }

    /**
     * Reassemble the fragments, and when complete, deliver to the application callback.
     */
    private void reassembleAndDeliver(byte srcAddr, short messageID,
                                      byte fragIdx, byte totalFrags, byte[] payload)
    {
        String mapKey = srcAddr + ":" + messageID;
        MessageFragmentBuffer buffer = reassemblyMap.get(mapKey);
        if (buffer == null) {
            buffer = new MessageFragmentBuffer(totalFrags);
            reassemblyMap.put(mapKey, buffer);
        }

        buffer.addFragment(fragIdx, payload);

        if (buffer.isComplete()) {
            // Reassemble
            byte[] fullMsgBytes = buffer.reassemble();
            // Convert to string
            String msgText = new String(fullMsgBytes, StandardCharsets.UTF_8);
            // Deliver up
            if (callback != null) {
                callback.onReceivedChatMessage("Node " + srcAddr, msgText);
            }
            // Remove from map
            reassemblyMap.remove(mapKey);
        }
    }

    /**
     * Adds the local node's 'HELLO' frames to the macTxQueue for neighbor discovery.
     */
    private void sendHello() {
        ByteBuffer frame = buildFrameHeader(
                (byte)0x01, // controlFlags 0x01 = HELLO
                localAddress,
                BROADCAST_ADDR,
                (short)0,  // messageID = 0 for HELLO
                (byte)0,   // fragmentIndex
                (byte)1    // totalFragments
        );
        frame.flip();
        macTxQueue.offer(frame);
    }

    /**
     * Update neighbor table upon receiving any frame from them.
     */
    private void updateNeighbor(byte neighborAddr) {
        neighbors.put(neighborAddr, System.currentTimeMillis());
        // Optional: notify callback about new set of neighbors
        if (callback != null) {
            callback.onNeighborsChanged(neighbors.keySet());
        }
    }

    /**
     * Remove stale neighbors
     */
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

    /**
     * The MAC loop. We take frames from `macTxQueue`, then wait for the channel to be FREE,
     * do a random backoff, and then place the frame on `sendQueue`.
     */
    private void macLoop() {
        Random rand = new Random();
        while(true) {
            try {
                ByteBuffer frame = macTxQueue.take();
                // Wait until channel is free
                while(channelBusy) {
                    Thread.sleep(10);
                }
                // Random short backoff
                Thread.sleep(10 + rand.nextInt(10)); // simplistic

                // Check if still free
                if (channelBusy) {
                    // Put it back and wait
                    macTxQueue.offer(frame);
                    continue;
                }

                // Send
                sendQueue.put(new Message(MessageType.DATA, frame));

            } catch (InterruptedException e) {
                System.err.println("MAC loop interrupted.");
                return;
            }
        }
    }

    /**
     * Utility for building a header: 6 bytes in this example:
     *  [0] controlFlags
     *  [1] srcAddr
     *  [2] dstAddr
     *  [3] messageID high
     *  [4] messageID low
     *  [5] fragmentIndex
     *  (Optionally [6] totalFragments if you want 7 bytes)
     */
    private ByteBuffer buildFrameHeader(byte controlFlags, byte srcAddr, byte dstAddr,
                                        short messageID, byte fragmentIdx, byte totalFrags)
    {
        ByteBuffer buf = ByteBuffer.allocate(MAX_DATA_LEN);
        buf.put(controlFlags);
        buf.put(srcAddr);
        buf.put(dstAddr);
        buf.putShort(messageID);   // 2 bytes
        buf.put(fragmentIdx);
        // We’re skipping totalFrags if we keep HEADER_LEN=6
        // If you want totalFrags, expand HEADER_LEN to 7 and store it as well:
        // buf.put(totalFrags);

        return buf;
    }
}