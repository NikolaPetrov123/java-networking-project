import client.*;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import protocol.ProtocolLayer;

/**
 * MyProtocol focusing on application-level logic.
 * Now it supports the possibility of dynamic addressing by passing "0" as localAddress.
 */
public class MyProtocol {

    private static String SERVER_IP = "netsys.ewi.utwente.nl";
    private static int SERVER_PORT = 8954;
    private static int frequency = 2720;
    private static String token = "java-22-B35DAB54DF6BBA640D";

    // If this is 0, ProtocolLayer will do dynamic addressing
    private static byte localAddress = 0x00;

    public static void main(String[] args) {
        // For example:
        //   args[0] = frequency
        //   args[1] = localAddress (if 0 => dynamic)
        if (args.length > 0) {
            frequency = Integer.parseInt(args[0]);
        }
        if (args.length > 1) {
            localAddress = (byte) Integer.parseInt(args[1]);
        }

        // Queues for messages from/to Client
        BlockingQueue<Message> receivedQueue = new LinkedBlockingQueue<>();
        BlockingQueue<Message> sendingQueue = new LinkedBlockingQueue<>();

        // Create the underlying client (connect to emulator)
        new Client(SERVER_IP, SERVER_PORT, frequency, token, receivedQueue, sendingQueue);

        // Create ProtocolLayer
        ProtocolLayer layer = new ProtocolLayer(
                localAddress,
                receivedQueue,
                sendingQueue,
                new ProtocolLayer.ProtocolCallback() {
                    @Override
                    public void onReceivedChatMessage(String fromAddr, String message) {
                        System.out.println("[" + fromAddr + "]: " + message);
                    }

                    @Override
                    public void onNeighborsChanged(Set<Byte> currentNeighbors) {
                        // Could display or log neighbor changes if desired
                    }
                }
        );

        layer.start();  // starts receiving loop, MAC loop, address assignment, etc.

        // Simple CLI
        Scanner sc = new Scanner(System.in);
        System.out.println("Commands: /list, /whisper <addr> <msg>, /quit");
        while (true) {
            String line = sc.nextLine();
            if (line.equalsIgnoreCase("/quit")) {
                System.out.println("Exiting...");
                System.exit(0);
            } else if (line.equalsIgnoreCase("/list")) {
                System.out.println("My Address: " + layer.getLocalAddress());
                System.out.println("Routing Table (reachable nodes):");
                System.out.println(layer.getRoutingTableString());
            }else if (line.startsWith("/whisper ")) {
                String[] parts = line.split(" ", 3);
                if (parts.length < 3) {
                    System.out.println("Usage: /whisper <destAddr> <message>");
                    continue;
                }
                byte dest = (byte) Integer.parseInt(parts[1]);
                String msg = parts[2];
                layer.sendChatMessage(msg, dest);
            } else {
                // broadcast
                layer.sendChatMessage(line, ProtocolLayer.BROADCAST_ADDR);
            }
        }
    }
}