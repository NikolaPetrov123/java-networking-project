package protocol;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * AckManager helps us block until a specific (messageID, fragmentIndex) is acknowledged.
 * We identify an ACK by a key: (srcAddr, messageID, fragmentIndex).
 *
 * For a "Stop-and-Wait" approach, we do:
 *  - Start waiting for ackKey
 *  - If we get an ACK, we notify and that unblocks the waiting thread.
 */
public class AckManager {

    // Map ackKey -> CountDownLatch(1)
    private ConcurrentHashMap<String, CountDownLatch> ackLatches = new ConcurrentHashMap<>();

    /**
     * Generate a consistent key for the awaiting sender:
     * We store the localAddress in place of "srcAddr" if we want an ACK for that node's message.
     *
     * Example usage: ackKey = makeAckKey(localAddr, msgID, fragIdx)
     */
    public String makeAckKey(byte srcAddr, short messageID, byte fragmentIndex) {
        return srcAddr + ":" + messageID + ":" + fragmentIndex;
    }

    /**
     * Wait for the ACK, up to timeoutMs.
     * Returns true if acknowledged, false if timed out.
     */
    public boolean waitForAck(String ackKey, long timeoutMs) throws InterruptedException {
        // Create a new latch if not already exist
        ackLatches.putIfAbsent(ackKey, new CountDownLatch(1));
        CountDownLatch latch = ackLatches.get(ackKey);
        boolean done = latch.await(timeoutMs, TimeUnit.MILLISECONDS);
        // If we timed out, done=false, latch not necessarily removed. We'll keep it around
        return done;
    }

    /**
     * Called when an ACK arrives for a given key.
     * We decrement the latch, allowing the waiting thread to proceed.
     */
    public void notifyAck(String ackKey) {
        CountDownLatch latch = ackLatches.get(ackKey);
        if (latch != null) {
            latch.countDown();
        }
    }
}