package filewire.client;

import filewire.protocol.DigestUtil;
import filewire.protocol.ErrorCode;
import filewire.protocol.ProtocolConstants;
import filewire.server.FileWireServer;
import filewire.server.ServerConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FileWireIntegrationTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void realServerRoundTripsBoundaryAndRandomFilesAndCleansUp() throws Exception {
        Path storage = temporaryDirectory.resolve("server-files");
        Path sources = Files.createDirectory(temporaryDirectory.resolve("sources"));
        Path downloads = temporaryDirectory.resolve("downloads");
        Map<String, byte[]> fixtures = fixtures();

        FileWireServer server = new FileWireServer(ServerConfig.defaults(storage, 0));
        server.start();
        try {
            try (FileWireClient client = FileWireClient.connect("127.0.0.1", server.port(), downloads)) {
                for (Map.Entry<String, byte[]> fixture : fixtures.entrySet()) {
                    Path source = sources.resolve(fixture.getKey());
                    Files.write(source, fixture.getValue());
                    TransferReceipt receipt = client.upload(source, fixture.getKey());
                    assertReceipt(receipt, source, fixture.getKey());
                }

                assertEquals(new TreeSet<>(fixtures.keySet()), new TreeSet<>(client.listFiles()));

                Path duplicateSource = sources.resolve(fixtures.keySet().iterator().next());
                RemoteOperationException duplicate = assertThrows(
                        RemoteOperationException.class,
                        () -> client.upload(duplicateSource, fixtures.keySet().iterator().next()));
                assertEquals(ErrorCode.FILE_ALREADY_EXISTS, duplicate.errorCode());
                assertTrue(client.isConnected());

                for (Map.Entry<String, byte[]> fixture : fixtures.entrySet()) {
                    Path relativeDestination = Path.of("copy-" + fixture.getKey());
                    TransferReceipt receipt = client.download(fixture.getKey(), relativeDestination);
                    Path downloaded = downloads.resolve(relativeDestination);
                    assertArrayEquals(fixture.getValue(), Files.readAllBytes(downloaded));
                    assertEquals(fixture.getValue().length, receipt.bytesTransferred());

                    assertThrows(
                            java.io.IOException.class,
                            () -> client.download(fixture.getKey(), relativeDestination));
                    assertTrue(client.isConnected());
                }

                for (String filename : fixtures.keySet()) {
                    client.delete(filename);
                }
                assertEquals(Set.of(), Set.copyOf(client.listFiles()));

                RemoteOperationException missing = assertThrows(
                        RemoteOperationException.class,
                        () -> client.delete("missing.bin"));
                assertEquals(ErrorCode.FILE_NOT_FOUND, missing.errorCode());
                assertTrue(client.isConnected());

                client.disconnect();
                assertFalse(client.isConnected());
            }
        } finally {
            server.close();
        }

        assertEquals(0, server.activeSessionCount());
        assertDirectoryEmpty(storage.resolve(".filewire-tmp"));
    }

    private static Map<String, byte[]> fixtures() {
        int chunk = ProtocolConstants.MAX_CHUNK_BYTES;
        Map<String, byte[]> fixtures = new LinkedHashMap<>();
        fixtures.put("empty.bin", new byte[0]);
        fixtures.put("one-byte.bin", randomBytes(1, 1));
        fixtures.put("chunk-minus-one.bin", randomBytes(chunk - 1, 2));
        fixtures.put("exact-chunk.bin", randomBytes(chunk, 3));
        fixtures.put("chunk-plus-one.bin", randomBytes(chunk + 1, 4));
        fixtures.put("two-chunks.bin", randomBytes(chunk * 2, 5));
        fixtures.put("multi-partial.bin", randomBytes(chunk * 3 + 137, 6));
        return fixtures;
    }

    private static byte[] randomBytes(int length, long seed) {
        byte[] bytes = new byte[length];
        new Random(seed).nextBytes(bytes);
        if (length > 2) {
            bytes[length / 2] = 0;
        }
        return bytes;
    }

    private static void assertReceipt(TransferReceipt receipt, Path source, String filename)
            throws Exception {
        assertEquals(filename, receipt.filename());
        assertEquals(Files.size(source), receipt.bytesTransferred());
        assertEquals(DigestUtil.toHex(DigestUtil.sha256(source)), receipt.sha256());
    }

    private static void assertDirectoryEmpty(Path directory) throws Exception {
        if (!Files.exists(directory)) {
            return;
        }
        try (Stream<Path> entries = Files.list(directory)) {
            assertTrue(entries.findAny().isEmpty(), "temporary upload directory is not empty");
        }
    }
}
