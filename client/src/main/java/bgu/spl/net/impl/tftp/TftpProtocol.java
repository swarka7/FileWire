package bgu.spl.net.impl.tftp;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

import bgu.spl.net.api.MessagingProtocol;

public class TftpProtocol implements MessagingProtocol<byte[]> {
    private final String basePath = Paths.get("").toAbsolutePath().toString();

    private boolean connected = false;
    private boolean sendingFile = false;
    private boolean sendFinalEmptyNeeded = false;
    private int sendBlockNumber = 0;
    private int sendOffset = 0;
    private byte[] sendBuffer;

    private boolean expectingDirq = false;
    private String downloadFilename;
    private int lastReceivedBlockNumber = 0;
    private final List<byte[]> receivedChunks = new ArrayList<>();
    private final Logger logger = Logger.getLogger(TftpProtocol.class.getName());

    public boolean iswrq = false;

    public boolean isConnected() {
        return connected;
    }

    @Override
    public byte[] process(byte[] message) {
        int opcode = parseShort(message, 0);
        logger.fine("Received opcode: " + opcode);
        switch (opcode) {
            case 3: // DATA
                int receivedBlock = handleDataPacket(message);
                return createAckPacket(receivedBlock);
            case 4: // ACK
                return ACK(message);
            case 5: // ERROR
                ERROR(message);
                break;
            case 9: // BCAST
                BCAST(message);
                break;
            default:
                return createErrorPacket(4, "Unknown Opcode");
        }
        return null;
    }

    @Override
    public boolean shouldTerminate() {
        return false;
    }

    public static boolean fileExists(String filename) {
        File file = new File(Paths.get("", filename).toString());
        return file.exists();
    }

    public void BCAST(byte[] message) {
        boolean isAdded = message[2] == 1;
        String filename = extractString(message, 3);
        String verb = isAdded ? "add" : "del";
        System.out.println("BCAST " + verb + ": " + filename);
    }

    public boolean WRQ(byte[] message) {
        String filenameStr = new String(message, StandardCharsets.UTF_8);
        if (!fileExists(filenameStr)) {
            System.out.println("file does not exists");
            return false;
        }

        sendBuffer = readFile(filenameStr);
        sendOffset = 0;
        sendBlockNumber = 0;
        sendFinalEmptyNeeded = (sendBuffer.length % 512 == 0);
        sendingFile = true;
        return true;
    }

    private byte[] readFile(String filename) {
        File file = new File(Paths.get("", filename).toString());
        byte[] data = new byte[(int) file.length()];
        try (FileInputStream fis = new FileInputStream(file)) {
            fis.read(data);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return data;
    }

    public void ERROR(byte[] message) {
        int errorCode = parseShort(message, 2);
        String errorMessage = extractString(message, 4);
        System.out.println("ERROR " + errorCode + " " + errorMessage);
    }

    public byte[] ACK(byte[] message) {
        if (!connected) {
            connected = true;
        }

        int receivedBlockNumber = parseShort(message, 2);
        System.out.println("ACK " + receivedBlockNumber);

        if (iswrq && sendingFile && receivedBlockNumber == sendBlockNumber) {
            byte[] next = nextDataPacket();
            if (next == null) {
                iswrq = false;
                sendingFile = false;
            }
            return next;
        }

        return null;
    }

    public int handleDataPacket(byte[] message) {
        int packetSize = parseShort(message, 2);
        int receivedBlockNumber = parseShort(message, 4);

        if (receivedBlockNumber != lastReceivedBlockNumber + 1) {
            return receivedBlockNumber;
        }

        byte[] rawData = Arrays.copyOfRange(message, 6, 6 + packetSize);
        receivedChunks.add(rawData);
        lastReceivedBlockNumber = receivedBlockNumber;

        if (packetSize < 512) {
            byte[] allData = joinChunks();
            if (expectingDirq) {
                printDirq(allData);
                expectingDirq = false;
            } else {
                writeDownloadedFile(allData);
                System.out.println("RRQ " + downloadFilename + " complete");
            }
            receivedChunks.clear();
            lastReceivedBlockNumber = 0;
        }

        return receivedBlockNumber;
    }

    public void CreateFile(String filename) {
        downloadFilename = new String(filename);
        lastReceivedBlockNumber = 0;
        receivedChunks.clear();
        expectingDirq = false;
    }

    public void startDirq() {
        expectingDirq = true;
        downloadFilename = null;
        lastReceivedBlockNumber = 0;
        receivedChunks.clear();
    }

    private void printDirq(byte[] data) {
        int start = 0;
        for (int i = 0; i < data.length; i++) {
            if (data[i] == 0) {
                if (i > start) {
                    String name = new String(data, start, i - start, StandardCharsets.UTF_8);
                    System.out.println(name);
                }
                start = i + 1;
            }
        }
    }

    private void writeDownloadedFile(byte[] data) {
        if (downloadFilename == null) {
            return;
        }
        File file = new File(basePath, downloadFilename);
        try {
            file.createNewFile();
            try (FileOutputStream outputStream = new FileOutputStream(file)) {
                outputStream.write(data);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private byte[] nextDataPacket() {
        if (!sendingFile) {
            return null;
        }

        if (sendOffset == sendBuffer.length) {
            if (!sendFinalEmptyNeeded) {
                return null;
            }
            sendFinalEmptyNeeded = false;
            sendBlockNumber++;
            return createDataPacket(sendBlockNumber, new byte[0]);
        }

        int remaining = sendBuffer.length - sendOffset;
        int chunkSize = Math.min(remaining, 512);
        byte[] chunk = Arrays.copyOfRange(sendBuffer, sendOffset, sendOffset + chunkSize);
        sendOffset += chunkSize;
        sendBlockNumber++;
        return createDataPacket(sendBlockNumber, chunk);
    }

    private static byte[] createDataPacket(int blockNumber, byte[] data) {
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

    private static byte[] createAckPacket(int blockNumber) {
        byte[] blockBytes = intToBytesBigEndian(blockNumber);
        return new byte[] { 0, 4, blockBytes[0], blockBytes[1] };
    }

    private static byte[] createErrorPacket(int errorCode, String errorMessage) {
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

    private static int parseShort(byte[] data, int offset) {
        return ((data[offset] & 0xFF) << 8) | (data[offset + 1] & 0xFF);
    }

    private static byte[] intToBytesBigEndian(int value) {
        return new byte[] { (byte) ((value >> 8) & 0xFF), (byte) (value & 0xFF) };
    }

    private static String extractString(byte[] data, int offset) {
        int end = offset;
        while (end < data.length && data[end] != 0) {
            end++;
        }
        return new String(data, offset, end - offset, StandardCharsets.UTF_8);
    }

    private byte[] joinChunks() {
        int totalLength = 0;
        for (byte[] chunk : receivedChunks) {
            totalLength += chunk.length;
        }
        byte[] all = new byte[totalLength];
        int index = 0;
        for (byte[] chunk : receivedChunks) {
            System.arraycopy(chunk, 0, all, index, chunk.length);
            index += chunk.length;
        }
        return all;
    }
}
