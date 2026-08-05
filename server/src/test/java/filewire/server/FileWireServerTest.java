package filewire.server;

import filewire.protocol.DigestUtil;
import filewire.protocol.ErrorCode;
import filewire.protocol.Frame;
import filewire.protocol.FrameCodec;
import filewire.protocol.MessageType;
import filewire.protocol.PayloadCodec;
import filewire.protocol.PayloadCodec.DownloadMetadata;
import filewire.protocol.PayloadCodec.DownloadRequest;
import filewire.protocol.PayloadCodec.FileChunk;
import filewire.protocol.PayloadCodec.UploadAccepted;
import filewire.protocol.PayloadCodec.UploadRequest;
import filewire.protocol.ProtocolConstants;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FileWireServerTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void bindsPortZeroAndCleansRegistryAfterGracefulDisconnectAndEof() throws Exception {
        FileWireServer server = server(2, 2);
        assertThrows(IllegalStateException.class, server::port);
        try (server) {
            server.start();
            assertTrue(server.port() > 0);
            assertTrue(server.isRunning());

            try (Socket graceful = connect(server)) {
                hello(graceful, 1);
                write(graceful, new Frame(MessageType.DISCONNECT, 2, new byte[0]));
                Frame response = FrameCodec.read(graceful.getInputStream());
                assertNotNull(response);
                assertEquals(MessageType.SUCCESS, response.type());
                assertEquals(2, response.correlationId());
            }
            await(Duration.ofSeconds(2), () -> server.activeSessionCount() == 0);

            Socket unexpectedEof = connect(server);
            hello(unexpectedEof, 3);
            assertEquals(1, server.activeSessionCount());
            unexpectedEof.close();
            await(Duration.ofSeconds(2), () -> server.activeSessionCount() == 0);
        }

        server.close();
        assertFalse(server.isRunning());
        assertEquals(0, server.activeSessionCount());
    }

    @Test
    void hardConnectionLimitReturnsStructuredBusyError() throws Exception {
        try (FileWireServer server = server(1, 1)) {
            server.start();
            try (Socket first = connect(server); Socket second = connect(server)) {
                await(Duration.ofSeconds(2), () -> server.activeSessionCount() == 2);

                try (Socket rejected = connect(server)) {
                    Frame rejection = FrameCodec.read(rejected.getInputStream());
                    assertNotNull(rejection);
                    assertEquals(MessageType.ERROR, rejection.type());
                    assertEquals(ErrorCode.SERVER_BUSY, PayloadCodec.decodeError(rejection.payload()).code());
                }
                assertEquals(server.maximumConnections(), server.activeSessionCount());
            }
        }
    }

    @Test
    void shutdownCancelsInterruptedUploadAndRemovesTemporaryFile() throws Exception {
        Path storageRoot = temporaryDirectory.resolve("storage");
        FileWireServer server = server(1, 2);
        Socket socket = null;
        try {
            server.start();
            socket = connect(server);
            hello(socket, 1);

            byte[] completeData = randomBytes(100);
            UploadRequest request = new UploadRequest("interrupted.bin", completeData.length, digest(completeData));
            write(socket, new Frame(
                    MessageType.UPLOAD_REQUEST,
                    2,
                    PayloadCodec.encodeUploadRequest(request)));
            Frame acceptedFrame = FrameCodec.read(socket.getInputStream());
            assertNotNull(acceptedFrame);
            UploadAccepted accepted = PayloadCodec.decodeUploadAccepted(acceptedFrame.payload());

            write(socket, new Frame(
                    MessageType.FILE_CHUNK,
                    accepted.transferId(),
                    PayloadCodec.encodeFileChunk(new FileChunk(
                            0,
                            false,
                            Arrays.copyOf(completeData, 40)))));
            await(Duration.ofSeconds(2), () -> countTempFiles(storageRoot) == 1);
        } finally {
            server.close();
            if (socket != null) {
                socket.close();
            }
        }

        assertFalse(Files.exists(storageRoot.resolve("interrupted.bin")));
        assertEquals(0, countTempFiles(storageRoot));
        assertEquals(0, server.activeSessionCount());
    }

    @Test
    void downloadPreservesChunkPlusPartialTailExactly() throws Exception {
        Path root = temporaryDirectory.resolve("storage");
        Files.createDirectories(root);
        byte[] expected = randomBytes(ProtocolConstants.MAX_CHUNK_BYTES + 1);
        Files.write(root.resolve("tail.bin"), expected);

        try (FileWireServer server = server(2, 2)) {
            server.start();
            try (Socket socket = connect(server)) {
                hello(socket, 1);
                write(socket, new Frame(
                        MessageType.DOWNLOAD_REQUEST,
                        2,
                        PayloadCodec.encodeDownloadRequest(new DownloadRequest("tail.bin"))));

                Frame metadataFrame = FrameCodec.read(socket.getInputStream());
                assertNotNull(metadataFrame);
                assertEquals(MessageType.DOWNLOAD_METADATA, metadataFrame.type());
                DownloadMetadata metadata = PayloadCodec.decodeDownloadMetadata(metadataFrame.payload());
                assertEquals(expected.length, metadata.size());

                ByteArrayOutputStream received = new ByteArrayOutputStream();
                Frame firstChunkFrame = FrameCodec.read(socket.getInputStream());
                Frame secondChunkFrame = FrameCodec.read(socket.getInputStream());
                FileChunk firstChunk = PayloadCodec.decodeFileChunk(firstChunkFrame.payload());
                FileChunk secondChunk = PayloadCodec.decodeFileChunk(secondChunkFrame.payload());
                received.write(firstChunk.data());
                received.write(secondChunk.data());

                assertEquals(metadata.transferId(), firstChunkFrame.correlationId());
                assertEquals(0, firstChunk.sequence());
                assertFalse(firstChunk.finalChunk());
                assertEquals(ProtocolConstants.MAX_CHUNK_BYTES, firstChunk.data().length);
                assertEquals(1, secondChunk.sequence());
                assertTrue(secondChunk.finalChunk());
                assertEquals(1, secondChunk.data().length);
                assertArrayEquals(expected, received.toByteArray());

                Frame complete = FrameCodec.read(socket.getInputStream());
                assertEquals(MessageType.TRANSFER_COMPLETE, complete.type());
                assertEquals(metadata.transferId(), complete.correlationId());
            }
        }
    }

    @Test
    void closeBeforeStartIsIdempotentAndPreventsRestart() throws Exception {
        FileWireServer server = server(1, 1);
        server.close();
        server.close();
        assertThrows(IllegalStateException.class, server::start);
        assertEquals(0, server.activeSessionCount());
    }

    private FileWireServer server(int workers, int queue) throws IOException {
        return new FileWireServer(new ServerConfig(
                temporaryDirectory.resolve("storage"),
                0,
                workers,
                queue,
                5_000,
                10_000_000,
                Duration.ofSeconds(2)));
    }

    private static Socket connect(FileWireServer server) throws IOException {
        Socket socket = new Socket(InetAddress.getLoopbackAddress(), server.port());
        socket.setSoTimeout(3_000);
        return socket;
    }

    private static void hello(Socket socket, long requestId) throws IOException {
        write(socket, new Frame(MessageType.HELLO, requestId, new byte[0]));
        Frame response = FrameCodec.read(socket.getInputStream());
        assertNotNull(response);
        assertEquals(MessageType.SUCCESS, response.type());
        assertEquals(requestId, response.correlationId());
    }

    private static void write(Socket socket, Frame frame) throws IOException {
        FrameCodec.write(socket.getOutputStream(), frame);
        socket.getOutputStream().flush();
    }

    private static void await(Duration timeout, BooleanSupplier condition) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            LockSupport.parkNanos(Duration.ofMillis(2).toNanos());
        }
        assertTrue(condition.getAsBoolean(), "Condition was not met before " + timeout);
    }

    private static int countTempFiles(Path storageRoot) {
        Path tempRoot = storageRoot.resolve(StorageService.TEMP_DIRECTORY_NAME);
        if (!Files.isDirectory(tempRoot)) {
            return 0;
        }
        try (var entries = Files.list(tempRoot)) {
            return Math.toIntExact(entries.count());
        } catch (IOException exception) {
            throw new AssertionError(exception);
        }
    }

    private static byte[] digest(byte[] data) throws IOException {
        return DigestUtil.sha256(new ByteArrayInputStream(data));
    }

    private static byte[] randomBytes(int size) {
        byte[] data = new byte[size];
        new Random(91_727L + size).nextBytes(data);
        return data;
    }
}
