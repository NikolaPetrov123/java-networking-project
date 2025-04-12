package protocol;

/**
 * Utility to hold multiple fragments for a single message.
 * Once all fragments arrive, we can reassemble into the full payload.
 */
public class MessageFragmentBuffer {

    private byte[][] fragments;
    private boolean[] received;
    private int totalFrags;
    private int receivedCount = 0;

    public MessageFragmentBuffer(int totalFrags) {
        this.totalFrags = totalFrags;
        this.fragments = new byte[totalFrags][];
        this.received = new boolean[totalFrags];
    }

    public void addFragment(int index, byte[] data) {
        if (index < 0 || index >= totalFrags) return;
        if (!received[index]) {
            fragments[index] = data;
            received[index] = true;
            receivedCount++;
        }
    }

    public boolean isComplete() {
        return (receivedCount == totalFrags);
    }

    public byte[] reassemble() {
        int totalBytes = 0;
        for (byte[] part : fragments) {
            totalBytes += part.length;
        }
        byte[] result = new byte[totalBytes];

        int offset = 0;
        for (byte[] part : fragments) {
            System.arraycopy(part, 0, result, offset, part.length);
            offset += part.length;
        }
        return result;
    }
}