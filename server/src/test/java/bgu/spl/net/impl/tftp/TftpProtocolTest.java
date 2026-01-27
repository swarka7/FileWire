package bgu.spl.net.impl.tftp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class TftpProtocolTest {

    private static final Path FILES_DIR = Paths.get("Files");

    @AfterEach
    public void cleanup() throws Exception {
        if (Files.exists(FILES_DIR)) {
            try (java.util.stream.Stream<Path> paths = Files.list(FILES_DIR)) {
                paths.forEach(path -> {
                    try {
                        Files.deleteIfExists(path);
                    } catch (Exception e) {
                        // ignore cleanup errors
                    }
                });
            }
        }
    }

    @Test
    public void loginSuccessAck0() {
        RecordingConnections connections = new RecordingConnections();
        TftpProtocol protocol = new TftpProtocol();
        protocol.start(1, connections);

        protocol.process(buildStringPacket(7, "alice"));

        byte[] last = connections.lastSent();
        assertEquals(4, parseShort(last, 0));
        assertEquals(0, parseShort(last, 2));
    }

    @Test
    public void duplicateLoginGetsError7() {
        RecordingConnections connections = new RecordingConnections();
        connections.addName(1, "bob");
        TftpProtocol protocol = new TftpProtocol();
        protocol.start(2, connections);

        protocol.process(buildStringPacket(7, "bob"));

        byte[] last = connections.lastSent();
        assertEquals(5, parseShort(last, 0));
        assertEquals(7, parseShort(last, 2));
    }

    @Test
    public void rrqMissingFileGetsError1() {
        RecordingConnections connections = new RecordingConnections();
        TftpProtocol protocol = new TftpProtocol();
        protocol.start(1, connections);
        protocol.process(buildStringPacket(7, "alice"));
        connections.clear();

        protocol.process(buildStringPacket(1, "missing.txt"));

        byte[] last = connections.lastSent();
        assertEquals(5, parseShort(last, 0));
        assertEquals(1, parseShort(last, 2));
    }

    @Test
    public void wrqExistingFileGetsError5() throws Exception {
        Files.createDirectories(FILES_DIR);
        Files.write(FILES_DIR.resolve("exists.txt"), "x".getBytes(StandardCharsets.UTF_8));

        RecordingConnections connections = new RecordingConnections();
        TftpProtocol protocol = new TftpProtocol();
        protocol.start(1, connections);
        protocol.process(buildStringPacket(7, "alice"));
        connections.clear();

        protocol.process(buildStringPacket(2, "exists.txt"));

        byte[] last = connections.lastSent();
        assertEquals(5, parseShort(last, 0));
        assertEquals(5, parseShort(last, 2));
    }

    @Test
    public void dirqWithoutLoginGetsError6() {
        RecordingConnections connections = new RecordingConnections();
        TftpProtocol protocol = new TftpProtocol();
        protocol.start(1, connections);

        protocol.process(new byte[] { 0, 6 });

        byte[] last = connections.lastSent();
        assertEquals(5, parseShort(last, 0));
        assertEquals(6, parseShort(last, 2));
    }

    private byte[] buildStringPacket(int opcode, String value) {
        byte[] nameBytes = value.getBytes(StandardCharsets.UTF_8);
        byte[] packet = new byte[nameBytes.length + 3];
        packet[0] = 0;
        packet[1] = (byte) opcode;
        System.arraycopy(nameBytes, 0, packet, 2, nameBytes.length);
        packet[packet.length - 1] = 0;
        return packet;
    }

    private int parseShort(byte[] data, int offset) {
        return ((data[offset] & 0xFF) << 8) | (data[offset + 1] & 0xFF);
    }

    private static class RecordingConnections extends bgu.spl.net.srv.ConnectionsImpl<byte[]> {
        private final List<byte[]> sent = new ArrayList<>();

        @Override
        public boolean send(int connectionId, byte[] msg) {
            sent.add(msg);
            return true;
        }

        public byte[] lastSent() {
            assertTrue(!sent.isEmpty());
            return sent.get(sent.size() - 1);
        }

        public void clear() {
            sent.clear();
        }
    }
}
