package bgu.spl.net.impl.tftp;

import java.io.ByteArrayOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import bgu.spl.net.api.BidiMessagingProtocol;
import bgu.spl.net.srv.ConnectionHandler;
import bgu.spl.net.srv.Connections;
import bgu.spl.net.srv.ConnectionsImpl;

public class TftpProtocol implements BidiMessagingProtocol<byte[]> {
    private int connectionId;
    private Connections<byte[]> connections;
    private ConnectionHandler<byte[]> handler;
    private boolean shouldTerminate = false;

    private boolean loggedIn = false;
    private String username;

    private byte[] sendBuffer = new byte[0];
    private int sendBlockNumber = 1;
    private boolean sendingData = false;
    private boolean lastSentWasFinal = false;

    private boolean receivingData = false;
    private int recvBlockNumber = 1;
    private String currentFilename;
    private final List<byte[]> receivedChunks = new ArrayList<>();

    private final FileStore fileStore = new FileStore(Paths.get("Files").toAbsolutePath());
    private final Logger logger = Logger.getLogger(TftpProtocol.class.getName());

    @Override
    public void start(int connectionId, Connections<byte[]> connections) {
        this.connectionId = connectionId;
        this.connections = connections;
        // FileStore constructor ensures base directory.
    }

    @Override
    public void process(byte[] message) {
        int opcode = parseShort(message, 0);
        logger.fine("Received opcode: " + opcode);

        switch (opcode) {
            case 1: // RRQ
                RRQ(message);
                break;
            case 2: // WRQ
                WRQ(message);
                break;
            case 3: // DATA
                handleDataPacket(message);
                break;
            case 4: // ACK
                ACK(message);
                break;
            case 6: // DIRQ
                DIRQ();
                break;
            case 7: // LOGRQ
                LOGRQ(message);
                break;
            case 8: // DELRQ
                DELRQ(message);
                break;
            case 10: // DISC
                DISC();
                break;
            default:
                connections.send(connectionId, createErrorPacket(4, "Unknown Opcode"));
        }
    }

    @Override
    public boolean shouldTerminate() {
        return shouldTerminate;
    }

    public void LOGRQ(byte[] message) {
        String name = extractString(message, 2);
        if (loggedIn || ((ConnectionsImpl<byte[]>) connections).containsName(name)) {
            connections.send(connectionId, createErrorPacket(7, "User already logged in"));
            return;
        }

        ((ConnectionsImpl<byte[]>) connections).addName(connectionId, name);
        loggedIn = true;
        username = name;
        logger.info("LOGRQ success: " + name);
        connections.send(connectionId, createAckPacket(0));
    }

    public void DELRQ(byte[] message) {
        if (!loggedIn) {
            connections.send(connectionId, createErrorPacket(6, "User not logged in"));
            return;
        }

        String filename = extractString(message, 2);
        if (!fileStore.exists(filename)) {
            connections.send(connectionId, createErrorPacket(1, "File not found"));
            return;
        }

        fileStore.delete(filename);
        logger.info("DELRQ deleted: " + filename);
        connections.send(connectionId, createAckPacket(0));
        sendBCAST(false, filename);
    }

    public void RRQ(byte[] message) {
        if (!loggedIn) {
            connections.send(connectionId, createErrorPacket(6, "User not logged in"));
            return;
        }

        String filename = extractString(message, 2);
        if (!fileStore.exists(filename)) {
            connections.send(connectionId, createErrorPacket(1, "File not found"));
            return;
        }

        byte[] data = fileStore.read(filename);
        currentFilename = filename;
        logger.info("RRQ start: " + filename);
        startSending(data);
    }

    public void WRQ(byte[] message) {
        if (!loggedIn) {
            connections.send(connectionId, createErrorPacket(6, "User not logged in"));
            return;
        }

        String filename = extractString(message, 2);
        if (fileStore.exists(filename)) {
            connections.send(connectionId, createErrorPacket(5, "File already exists"));
            return;
        }

        currentFilename = filename;
        receivingData = true;
        recvBlockNumber = 1;
        receivedChunks.clear();
        logger.info("WRQ start: " + filename);
        connections.send(connectionId, createAckPacket(0));
    }

    public void DIRQ() {
        if (!loggedIn) {
            connections.send(connectionId, createErrorPacket(6, "User not logged in"));
            return;
        }

        StringBuilder fileList = new StringBuilder();
        for (String name : fileStore.list()) {
            fileList.append(name).append((char) 0);
        }
        logger.info("DIRQ start");
        startSending(fileList.toString().getBytes(StandardCharsets.UTF_8));
    }

    public void ACK(byte[] message) {
        if (!loggedIn) {
            connections.send(connectionId, createErrorPacket(6, "User not logged in"));
            return;
        }

        int receivedBlockNumber = parseShort(message, 2);
        if (sendingData && receivedBlockNumber == sendBlockNumber - 1) {
            if (lastSentWasFinal) {
                sendingData = false;
            } else {
                sendNextDataBlock();
            }
        }
    }

    public void handleDataPacket(byte[] message) {
        if (!loggedIn) {
            connections.send(connectionId, createErrorPacket(6, "User not logged in"));
            return;
        }
        if (!receivingData) {
            return;
        }

        int packetSize = parseShort(message, 2);
        int receivedBlockNumber = parseShort(message, 4);

        if (receivedBlockNumber == recvBlockNumber - 1) {
            connections.send(connectionId, createAckPacket(receivedBlockNumber));
            return;
        }

        if (receivedBlockNumber != recvBlockNumber) {
            return;
        }

        byte[] data = Arrays.copyOfRange(message, 6, 6 + packetSize);
        receivedChunks.add(data);
        connections.send(connectionId, createAckPacket(receivedBlockNumber));
        recvBlockNumber++;

        if (packetSize < 512) {
            writeFileFromChunks();
            receivingData = false;
            logger.info("WRQ complete: " + currentFilename);
            sendBCAST(true, currentFilename);
        }
    }

    public void DISC() {
        if (!loggedIn) {
            connections.send(connectionId, createErrorPacket(6, "User not logged in"));
            return;
        }

        logger.info("DISC: " + username);
        connections.send(connectionId, createAckPacket(0));
        connections.disconnect(connectionId);
        loggedIn = false;
        shouldTerminate = true;
    }

    public void setHandler(BlockingConnectionHandler handler) {
        this.handler = handler;
        connections.connect(connectionId, handler);
    }

    private void startSending(byte[] data) {
        sendBuffer = data;
        sendBlockNumber = 1;
        sendingData = true;
        lastSentWasFinal = false;
        sendNextDataBlock();
    }

    private void sendNextDataBlock() {
        if (!sendingData) {
            return;
        }

        int offset = (sendBlockNumber - 1) * 512;
        int remaining = sendBuffer.length - offset;
        if (remaining < 0) {
            sendingData = false;
            return;
        }

        int chunkSize = Math.min(Math.max(remaining, 0), 512);
        byte[] chunk = Arrays.copyOfRange(sendBuffer, offset, offset + chunkSize);
        byte[] dataPacket = createDataPacket(sendBlockNumber, chunk);
        lastSentWasFinal = chunkSize < 512;
        connections.send(connectionId, dataPacket);
        sendBlockNumber++;
    }

    private byte[] createDataPacket(int blockNumber, byte[] data) {
        byte[] packet = new byte[6 + data.length];
        byte[] sizeBytes = intToBytesBigEndian(data.length);
        byte[] blockBytes = intToBytesBigEndian(blockNumber);
        packet[0] = 0;
        packet[1] = 3;
        packet[2] = sizeBytes[0];
        packet[3] = sizeBytes[1];
        packet[4] = blockBytes[0];
        packet[5] = blockBytes[1];
        System.arraycopy(data, 0, packet, 6, data.length);
        return packet;
    }

    private byte[] createAckPacket(int blockNumber) {
        byte[] blockBytes = intToBytesBigEndian(blockNumber);
        return new byte[] { 0, 4, blockBytes[0], blockBytes[1] };
    }

    private byte[] createErrorPacket(int errorCode, String errorMessage) {
        byte[] errorBytes = errorMessage.getBytes(StandardCharsets.UTF_8);
        byte[] codeBytes = intToBytesBigEndian(errorCode);
        byte[] packet = new byte[4 + errorBytes.length + 1];
        packet[0] = 0;
        packet[1] = 5;
        packet[2] = codeBytes[0];
        packet[3] = codeBytes[1];
        System.arraycopy(errorBytes, 0, packet, 4, errorBytes.length);
        packet[packet.length - 1] = 0;
        return packet;
    }

    public void sendBCAST(boolean isAdded, String filename) {
        byte[] filenameBytes = filename.getBytes(StandardCharsets.UTF_8);
        byte[] packet = new byte[3 + filenameBytes.length + 1];
        packet[0] = 0;
        packet[1] = 9;
        packet[2] = (byte) (isAdded ? 1 : 0);
        System.arraycopy(filenameBytes, 0, packet, 3, filenameBytes.length);
        packet[packet.length - 1] = 0;

        for (Integer id : ((ConnectionsImpl<byte[]>) connections).getLoggedInClients()) {
            connections.send(id, packet);
        }
    }

    private void writeFileFromChunks() {
        int totalSize = receivedChunks.stream().mapToInt(bytes -> bytes.length).sum();
        ByteArrayOutputStream out = new ByteArrayOutputStream(totalSize);
        for (byte[] chunk : receivedChunks) {
            out.write(chunk, 0, chunk.length);
        }
        try {
            fileStore.write(currentFilename, out.toByteArray());
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Failed to write file: " + currentFilename, e);
        }
        receivedChunks.clear();
    }

    private static byte[] intToBytesBigEndian(int value) {
        return new byte[] { (byte) ((value >> 8) & 0xFF), (byte) (value & 0xFF) };
    }

    private static int parseShort(byte[] data, int offset) {
        return ((data[offset] & 0xFF) << 8) | (data[offset + 1] & 0xFF);
    }

    private static String extractString(byte[] data, int offset) {
        int end = offset;
        while (end < data.length && data[end] != 0) {
            end++;
        }
        return new String(data, offset, end - offset, StandardCharsets.UTF_8);
    }
}
