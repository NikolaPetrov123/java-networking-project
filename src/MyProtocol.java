import client.*;

import java.nio.ByteBuffer;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.Set;
import java.util.Collections;
import java.util.HashSet;

/**
 * MyProtocol using DATA messages with a prefix to announce static addresses.
 *
 * Each node periodically sends an announcement: "ADDR:<staticAddress>"
 * Nodes listen for these announcements and update their neighbor list accordingly.
 * This version does not use regex for extraction; it simply uses the known fixed prefix
 * and the length of the static address to extract the announcement.
 */
public class MyProtocol {

    // Connection parameters.
    private static String SERVER_IP = "netsys.ewi.utwente.nl"; // or "127.0.0.1" for local testing
    private static int SERVER_PORT = 8954;
    private static int frequency = 2720; // TODO: Set this to your group frequency
    // Token for your frequency range.
    String token = "java-22-B35DAB54DF6BBA640D";

    // The static address for this node.
    private String staticAddress;

    private BlockingQueue<Message> receivedQueue;
    private BlockingQueue<Message> sendingQueue;

    // Set for storing neighbor addresses.
    private Set<String> neighbors = Collections.synchronizedSet(new HashSet<>());
    // Indicates whether the handshake (i.e. token acceptance) is complete.
    private volatile boolean tokenAccepted = false;

    public MyProtocol(String server_ip, int server_port, int frequency, String staticAddress) {
        this.staticAddress = staticAddress;

        receivedQueue = new LinkedBlockingQueue<Message>();
        sendingQueue = new LinkedBlockingQueue<Message>();

        // Initialize the client (this sets up sender and listener threads).
        new Client(SERVER_IP, SERVER_PORT, frequency, token, receivedQueue, sendingQueue);

        // Start a thread to process received messages.
        new ReceiveThread(receivedQueue, staticAddress).start();

        // After token acceptance, immediately send the address announcement.
        new Thread(() -> {
            try {
                while (!tokenAccepted) {
                    Thread.sleep(100);
                }
                sendAddressAnnouncement();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }).start();

        // Periodically re-send the address announcement (every 10 seconds) to re-advertise presence.
        new Thread(() -> {
            try {
                while (true) {
                    if (tokenAccepted) {
                        sendAddressAnnouncement();
                    }
                    Thread.sleep(10000);
                }
            } catch (InterruptedException e) {
                // Handle thread interruption if needed.
            }
        }).start();

        // Main thread: handle user input from standard input.
        try {
            ByteBuffer temp = ByteBuffer.allocate(1024);
            int read;
            int newLineOffset = 0;
            while (true) {
                read = System.in.read(temp.array());
                if (read > 0) {
                    String input = new String(temp.array(), 0, read).trim();
                    // "list" prints the neighbor table.
                    if (input.equalsIgnoreCase("list")) {
                        System.out.println("Known neighbors:");
                        synchronized (neighbors) {
                            for (String addr : neighbors) {
                                System.out.println(addr);
                            }
                        }
                        continue;
                    }
                    // Remove newline/carriage-return characters.
                    if (temp.get(read - 1) == '\n' || temp.get(read - 1) == '\r')
                        newLineOffset = 1;
                    if (read > 1 && (temp.get(read - 2) == '\n' || temp.get(read - 2) == '\r'))
                        newLineOffset = 2;
                    ByteBuffer toSend = ByteBuffer.allocate(read - newLineOffset);
                    toSend.put(temp.array(), 0, read - newLineOffset);
                    toSend.flip();

                    Message msg;
                    // Send as DATA if longer than 2 bytes; otherwise as DATA_SHORT.
                    if ((read - newLineOffset) > 2) {
                        msg = new Message(MessageType.DATA, toSend);
                    } else {
                        msg = new Message(MessageType.DATA_SHORT, toSend);
                    }
                    sendingQueue.put(msg);
                }
            }
        } catch (InterruptedException e) {
            System.exit(2);
        } catch (IOException e) {
            System.exit(2);
        }
    }

    /**
     * Sends an announcement using a DATA message that contains a string with the prefix "ADDR:".
     */
    private void sendAddressAnnouncement() {
        String announcement = "ADDR:" + staticAddress;
        ByteBuffer buf = ByteBuffer.wrap(announcement.getBytes(StandardCharsets.UTF_8));
        try {
            sendingQueue.put(new Message(MessageType.DATA, buf));
            System.out.println("Sent address announcement: " + announcement);
        } catch (InterruptedException e) {
            System.err.println("Failed to send address announcement: " + e);
        }
    }

    public static void main(String args[]) {
        if (args.length > 0) {
            frequency = Integer.parseInt(args[0]);
        }
        String staticAddr = "node4"; // Default address.
        if (args.length > 1) {
            staticAddr = args[1];
        }
        new MyProtocol(SERVER_IP, SERVER_PORT, frequency, staticAddr);
    }

    /**
     * The ReceiveThread processes incoming messages.
     * If a received DATA or DATA_SHORT message contains an address announcement starting with "ADDR:",
     * we use a fixed substring extraction (knowing the length of our static address) to update the neighbor list.
     */
    private class ReceiveThread extends Thread {
        private BlockingQueue<Message> receivedQueue;
        private String staticAddress;

        public ReceiveThread(BlockingQueue<Message> receivedQueue, String staticAddress) {
            super();
            this.receivedQueue = receivedQueue;
            this.staticAddress = staticAddress;
        }

        public void run() {
            while (true) {
                try {
                    Message m = receivedQueue.take();
                    switch (m.getType()) {
                        case TOKEN_ACCEPTED:
                            System.out.println("Token Valid!");
                            tokenAccepted = true;
                            break;
                        case TOKEN_REJECTED:
                            System.out.println("Token Rejected!");
                            break;
                        case HELLO:
                            System.out.println("HELLO");
                            break;
                        case FREE:
                            System.out.println("FREE");
                            break;
                        case BUSY:
                            System.out.println("BUSY");
                            break;
                        case SENDING:
                            System.out.println("SENDING");
                            break;
                        case DONE_SENDING:
                            System.out.println("DONE_SENDING");
                            break;
                        case END:
                            System.out.println("END received");
                            break;
                        case DATA:
                        case DATA_SHORT:
                            // Convert the ByteBuffer to a string.
                            ByteBuffer data = m.getData();
                            data.rewind();
                            byte[] bytes = new byte[data.remaining()];
                            data.get(bytes);
                            String messageText = new String(bytes, StandardCharsets.UTF_8).trim();

                            // Check if the message is an address announcement.
                            if (messageText.startsWith("ADDR:")) {
                                // Extract exactly the intended static address.
                                // We know the announcement format is "ADDR:" followed immediately by the address.
                                // So take the substring of length equal to "ADDR:".length() + staticAddress.length().
                                String addr = messageText.substring(5,10).trim();
                                if(!addr.equals(staticAddress)){
                                    if(neighbors.add(addr)){
                                        System.out.println("Received address announcement from: " + addr);
                                    }
                                }
                            } else {
                                System.out.println("DATA: " + messageText);
                            }
                            break;
                        default:
                            System.out.println("Unknown message type received.");
                            break;
                    }
                } catch (InterruptedException e) {
                    System.err.println("Failed to take from queue: " + e);
                }
            }
        }
    }
}