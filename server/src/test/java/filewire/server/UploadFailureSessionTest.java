package filewire.server;

import filewire.protocol.DigestUtil;
import filewire.protocol.ErrorCode;
import filewire.protocol.Frame;
import filewire.protocol.FrameCodec;
import filewire.protocol.MessageType;
import filewire.protocol.PayloadCodec;
import filewire.protocol.PayloadCodec.FileChunk;
import filewire.protocol.PayloadCodec.UploadAccepted;
import filewire.protocol.PayloadCodec.UploadRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.locks.LockSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UploadFailureSessionTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void rejectedChunkGetsOneStructuredErrorThenConnectionAndTemporaryFileClose() throws Exception {
        Path storage = temporaryDirectory.resolve("storage");
        ServerConfig config = new ServerConfig(
                storage, 0, 1, 1, 5_000, 1_000_000, Duration.ofSeconds(2));
        byte[] content = {1, 2, 3};

        try (FileWireServer server = new FileWireServer(config)) {
            server.start();
            try (Socket socket = new Socket(InetAddress.getLoopbackAddress(), server.port())) {
                socket.setSoTimeout(3_000);
                write(socket, new Frame(MessageType.HELLO, 1, new byte[0]));
                assertEquals(MessageType.SUCCESS, FrameCodec.read(socket.getInputStream()).type());

                byte[] digest = DigestUtil.sha256(new ByteArrayInputStream(content));
                write(socket, new Frame(
                        MessageType.UPLOAD_REQUEST,
                        2,
                        PayloadCodec.encodeUploadRequest(new UploadRequest("bad.bin", content.length, digest))));
                Frame acceptedFrame = FrameCodec.read(socket.getInputStream());
                assertNotNull(acceptedFrame);
                UploadAccepted accepted = PayloadCodec.decodeUploadAccepted(acceptedFrame.payload());

                write(socket, new Frame(
                        MessageType.FILE_CHUNK,
                        accepted.transferId(),
                        PayloadCodec.encodeFileChunk(new FileChunk(1, true, content))));
                Frame errorFrame = FrameCodec.read(socket.getInputStream());
                assertNotNull(errorFrame);
                assertEquals(MessageType.ERROR, errorFrame.type());
                assertEquals(accepted.transferId(), errorFrame.correlationId());
                assertEquals(ErrorCode.INVALID_REQUEST, PayloadCodec.decodeError(errorFrame.payload()).code());
                assertNull(FrameCodec.read(socket.getInputStream()));
            }

            await(Duration.ofSeconds(2), () -> server.activeSessionCount() == 0);
            assertFalse(Files.exists(storage.resolve("bad.bin")));
            try (var parts = Files.list(storage.resolve(StorageService.TEMP_DIRECTORY_NAME))) {
                assertEquals(0, parts.count());
            }
        }
    }

    private static void write(Socket socket, Frame frame) throws Exception {
        FrameCodec.write(socket.getOutputStream(), frame);
        socket.getOutputStream().flush();
    }

    private static void await(Duration timeout, java.util.function.BooleanSupplier condition) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            LockSupport.parkNanos(Duration.ofMillis(2).toNanos());
        }
        assertTrue(condition.getAsBoolean(), "Condition was not met before " + timeout);
    }
}
