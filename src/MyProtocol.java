import client.*;
import java.nio.ByteBuffer;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.Scanner;
import java.util.Set;
import protocol.ProtocolLayer;

/**
 * Refactored MyProtocol focusing on application-level logic.
 */
public class MyProtocol {

    private static String SERVER_IP = "netsys.ewi.utwente.nl";
    private static int SERVER_PORT = 8954;
    private static int frequency = 2720;
    private static String token = "java-22-B35DAB54DF6BBA640D";

    // Example: store local address as a byte, e.g. 0x01, 0x02, etc.
    // In real usage, parse from args
    private static byte localAddress = 0x01;

    public static void main(String[] args) {
        // parse arguments if needed
        if (args.length > 0) {
            frequency = Integer.parseInt(args[0]);
        }
        if (args.length > 1) {
            localAddress = (byte)Integer.parseInt(args[1]);
        }

        // Setup queues
        BlockingQueue<Message> receivedQueue = new LinkedBlockingQueue<>();
        BlockingQueue<Message> sendingQueue = new LinkedBlockingQueue<>();

        // Create the underlying client (physical connection)
        new Client(SERVER_IP, SERVER_PORT, frequency, token, receivedQueue, sendingQueue);

        // Create the ProtocolLayer
        ProtocolLayer layer = new ProtocolLayer(
                localAddress,
                receivedQueue,
                sendingQueue,
                new ProtocolLayer.ProtocolCallback() {
                    @Override
                    public void onReceivedChatMessage(String fromAddr, String message) {
                        System.out.println("[" + fromAddr + "] " + message);
                    }
                    @Override
                    public void onNeighborsChanged(Set<Byte> currentNeighbors) {
                        // Optional: do something if needed
                    }
                }
        );

        layer.start();

        // Simple CLI
        Scanner sc = new Scanner(System.in);
        System.out.println("Enter commands or messages. Type '/list' to list neighbors, '/quit' to exit.");
        while (true) {
            String line = sc.nextLine();
            if (line.equalsIgnoreCase("/quit")) {
                System.out.println("Exiting...");
                System.exit(0);
            } else if (line.equalsIgnoreCase("/list")) {
                Set<Byte> neighs = layer.getNeighbors();
                System.out.println("Neighbors: " + neighs);
            } else if (line.startsWith("/whisper ")) {
                // e.g. "/whisper 2 Hello"
                String[] parts = line.split(" ", 3);
                if (parts.length < 3) {
                    System.out.println("Usage: /whisper <destAddr> <message>");
                    continue;
                }
                byte dest = (byte)Integer.parseInt(parts[1]);
                String msg = parts[2];
                layer.sendChatMessage(msg, dest);
            } else {
                // Broadcast
                layer.sendChatMessage(line, (byte)0xFF); // 0xFF = broadcast
            }
        }
    }
}