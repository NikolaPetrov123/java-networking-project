package client;

import java.nio.channels.SocketChannel;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;

public class Client {

    private SocketChannel sock;
    private BlockingQueue<Message> receivedQueue;
    private BlockingQueue<Message> sendingQueue;
    private String token;

    public Client(String server_ip, int server_port, int frequency, String token,
                  BlockingQueue<Message> receivedQueue, BlockingQueue<Message> sendingQueue) {
        this.receivedQueue = receivedQueue;
        this.sendingQueue = sendingQueue;
        this.token = token;
        try {
            sock = SocketChannel.open();
            sock.connect(new InetSocketAddress(server_ip, server_port));
            Listener listener = new Listener(sock, receivedQueue);
            Sender sender = new Sender(sock, sendingQueue);

            // Send initial handshake messages.
            sender.sendConnect(frequency);
            sender.sendToken(token);

            // Start the sender and listener threads.
            listener.start();
            sender.start();
        } catch (IOException e) {
            System.err.println("Failed to connect: " + e);
            System.exit(1);
        }
    }

    private class Sender extends Thread {
        private BlockingQueue<Message> sendingQueue;
        private SocketChannel sock;

        public Sender(SocketChannel sock, BlockingQueue<Message> sendingQueue) {
            super();
            this.sendingQueue = sendingQueue;
            this.sock = sock;
        }

        private void senderLoop() {
            while (sock.isConnected()) {
                try {
                    Message msg = sendingQueue.take();
                    // Process only DATA or DATA_SHORT messages.
                    if (msg.getType() == MessageType.DATA || msg.getType() == MessageType.DATA_SHORT) {
                        ByteBuffer data = msg.getData();
                        data.rewind(); // Ensure the position is at the beginning.
                        int length = data.remaining();  // Use the exact number of bytes intended.
                        // Allocate a new ByteBuffer to hold a 1-byte header, 1-byte length, and the data.
                        ByteBuffer toSend = ByteBuffer.allocate(length + 2);
                        byte header;
                        if (msg.getType() == MessageType.DATA) {
                            header = 0x03;
                        } else { // DATA_SHORT
                            header = 0x06;
                        }
                        // Build the frame.
                        toSend.put(header);
                        toSend.put((byte) length);
                        toSend.put(data);
                        toSend.flip();
                        sock.write(toSend);
                    }
                    // (Other message types such as ADDRESS are handled in the application using DATA messages.)
                } catch (IOException e) {
                    System.err.println("Error in socket (sender): " + e.toString());
                } catch (InterruptedException e) {
                    System.err.println("Failed to take from sendingQueue: " + e.toString());
                }
            }
        }

        public void sendConnect(int frequency) {
            ByteBuffer buff = ByteBuffer.allocate(4);
            buff.put((byte) 9);
            buff.put((byte) ((frequency >> 16) & 0xff));
            buff.put((byte) ((frequency >> 8) & 0xff));
            buff.put((byte) (frequency & 0xff));
            buff.flip();
            try {
                sock.write(buff);
            } catch (IOException e) {
                System.err.println("Failed to send HELLO");
            }
        }

        public void sendToken(String token) {
            byte[] tokenBytes = token.getBytes();
            ByteBuffer buff = ByteBuffer.allocate(tokenBytes.length + 2);
            buff.put((byte) 10);
            buff.put((byte) tokenBytes.length);
            buff.put(tokenBytes);
            buff.flip();
            try {
                sock.write(buff);
            } catch (IOException e) {
                System.err.println("Failed to send TOKEN");
            }
        }

        public void run() {
            senderLoop();
        }
    }

    private class Listener extends Thread {
        private BlockingQueue<Message> receivedQueue;
        private SocketChannel sock;
        private ByteBuffer messageBuffer = ByteBuffer.allocate(1024);
        private int messageLength = -1;
        private boolean messageReceiving = false;
        private boolean shortData = false;

        public Listener(SocketChannel sock, BlockingQueue<Message> receivedQueue) {
            super();
            this.receivedQueue = receivedQueue;
            this.sock = sock;
        }

        private void parseMessage(ByteBuffer received, int bytesReceived) {
            try {
                for (int offset = 0; offset < bytesReceived; offset++) {
                    byte d = received.get(offset);
                    if (messageReceiving) {
                        if (messageLength == -1) {
                            messageLength = (int) d;
                            messageBuffer = ByteBuffer.allocate(messageLength);
                        } else {
                            messageBuffer.put(d);
                        }
                        if (messageBuffer.position() == messageLength) {
                            messageBuffer.flip();
                            ByteBuffer temp = ByteBuffer.allocate(messageLength);
                            temp.put(messageBuffer);
                            temp.flip();
                            if (shortData) {
                                receivedQueue.put(new Message(MessageType.DATA_SHORT, temp));
                            } else {
                                receivedQueue.put(new Message(MessageType.DATA, temp));
                            }
                            messageReceiving = false;
                        }
                    } else {
                        if (d == 0x09) { // HELLO
                            receivedQueue.put(new Message(MessageType.HELLO));
                        } else if (d == 0x01) { // FREE
                            receivedQueue.put(new Message(MessageType.FREE));
                        } else if (d == 0x02) { // BUSY
                            receivedQueue.put(new Message(MessageType.BUSY));
                        } else if (d == 0x03) { // DATA
                            messageLength = -1;
                            messageReceiving = true;
                            shortData = false;
                        } else if (d == 0x04) { // SENDING
                            receivedQueue.put(new Message(MessageType.SENDING));
                        } else if (d == 0x05) { // DONE_SENDING
                            receivedQueue.put(new Message(MessageType.DONE_SENDING));
                        } else if (d == 0x06) { // DATA_SHORT
                            messageLength = -1;
                            messageReceiving = true;
                            shortData = true;
                        } else if (d == 0x08) { // END
                            receivedQueue.put(new Message(MessageType.END));
                        } else if (d == 0x0A) { // TOKEN_ACCEPTED
                            receivedQueue.put(new Message(MessageType.TOKEN_ACCEPTED));
                        } else if (d == 0x0B) { // TOKEN_REJECTED
                            receivedQueue.put(new Message(MessageType.TOKEN_REJECTED));
                        }
                    }
                }
            } catch (InterruptedException e) {
                System.err.println("Failed to put data in receivedQueue: " + e.toString());
            }
        }

        public void receivingLoop() {
            int bytesRead;
            ByteBuffer recv = ByteBuffer.allocate(1024);
            try {
                while (sock.isConnected()) {
                    bytesRead = sock.read(recv);
                    if (bytesRead > 0) {
                        parseMessage(recv, bytesRead);
                    } else {
                        break;
                    }
                    recv.clear();
                }
            } catch (IOException e) {
                System.err.println("Error in socket (receiver): " + e.toString());
            }
        }

        public void run() {
            receivingLoop();
        }
    }
}