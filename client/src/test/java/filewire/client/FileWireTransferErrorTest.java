package filewire.client;

import filewire.protocol.DigestUtil;
import filewire.protocol.ErrorCode;
import filewire.protocol.Frame;
import filewire.protocol.FrameCodec;
import filewire.protocol.MessageType;
import filewire.protocol.PayloadCodec;
import filewire.protocol.ProtocolConstants;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FileWireTransferErrorTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void uploadErrorAfterAcceptanceMarksConnectionUnusable() throws Exception {
        byte[] content = {1, 2, 3, 4};
        Path source = temporaryDirectory.resolve("source.bin");
        Files.write(source, content);

        runScriptedPeer(socket -> {
            acceptHello(socket);
            requireFrame(socket, MessageType.KEEPALIVE);
            Frame request = requireFrame(socket, MessageType.UPLOAD_REQUEST);
            long transferId = 77;
            write(socket, new Frame(
                    MessageType.UPLOAD_ACCEPTED,
                    request.correlationId(),
                    PayloadCodec.encodeUploadAccepted(new PayloadCodec.UploadAccepted(
                            transferId, ProtocolConstants.MAX_CHUNK_BYTES))));
            requireFrame(socket, MessageType.FILE_CHUNK);
            writeError(socket, transferId, ErrorCode.INTEGRITY_MISMATCH, "Rejected upload chunk");
        }, port -> {
            try (FileWireClient client = FileWireClient.connect(
                    "127.0.0.1", port, temporaryDirectory.resolve("downloads"))) {
                RemoteOperationException failure = assertThrows(
                        RemoteOperationException.class,
                        () -> client.upload(source, "remote.bin"));
                assertEquals(ErrorCode.INTEGRITY_MISMATCH, failure.errorCode());
                assertFalse(client.isConnected());
            }
        });
    }

    @Test
    void downloadErrorAfterMetadataMarksConnectionUnusableAndCleansPart() throws Exception {
        byte[] expected = {5, 6, 7};
        Path downloadRoot = temporaryDirectory.resolve("downloads");

        runScriptedPeer(socket -> {
            acceptHello(socket);
            Frame request = requireFrame(socket, MessageType.DOWNLOAD_REQUEST);
            long transferId = 88;
            write(socket, new Frame(MessageType.KEEPALIVE, request.correlationId(), new byte[0]));
            write(socket, new Frame(
                    MessageType.DOWNLOAD_METADATA,
                    request.correlationId(),
                    PayloadCodec.encodeDownloadMetadata(new PayloadCodec.DownloadMetadata(
                            transferId,
                            "remote.bin",
                            expected.length,
                            DigestUtil.sha256(new java.io.ByteArrayInputStream(expected)),
                            ProtocolConstants.MAX_CHUNK_BYTES))));
            writeError(socket, transferId, ErrorCode.IO_FAILURE, "Download failed after metadata");
        }, port -> {
            try (FileWireClient client = FileWireClient.connect("127.0.0.1", port, downloadRoot)) {
                RemoteOperationException failure = assertThrows(
                        RemoteOperationException.class,
                        () -> client.download("remote.bin", Path.of("copy.bin")));
                assertEquals(ErrorCode.IO_FAILURE, failure.errorCode());
                assertFalse(client.isConnected());
                assertFalse(Files.exists(downloadRoot.resolve("copy.bin")));
                try (var entries = Files.list(downloadRoot)) {
                    assertTrue(entries.findAny().isEmpty());
                }
            }
        });
    }

    @Test
    void downloadMetadataAboveLocalLimitIsRejectedBeforeAnyFileBytes() throws Exception {
        Path downloadRoot = temporaryDirectory.resolve("limited-downloads");

        runScriptedPeer(socket -> {
            acceptHello(socket);
            Frame request = requireFrame(socket, MessageType.DOWNLOAD_REQUEST);
            write(socket, new Frame(
                    MessageType.DOWNLOAD_METADATA,
                    request.correlationId(),
                    PayloadCodec.encodeDownloadMetadata(new PayloadCodec.DownloadMetadata(
                            99,
                            "too-large.bin",
                            4,
                            new byte[ProtocolConstants.SHA256_BYTES],
                            ProtocolConstants.MAX_CHUNK_BYTES))));
        }, port -> {
            try (FileWireClient client = FileWireClient.connect(
                    "127.0.0.1", port, downloadRoot, 3)) {
                IOException failure = assertThrows(
                        IOException.class,
                        () -> client.download("too-large.bin", Path.of("copy.bin")));
                assertTrue(failure.getMessage().contains("configured 3-byte limit"));
                assertFalse(client.isConnected());
                assertFalse(Files.exists(downloadRoot.resolve("copy.bin")));
                try (var entries = Files.list(downloadRoot)) {
                    assertTrue(entries.findAny().isEmpty());
                }
            }
        });
    }

    private static void runScriptedPeer(PeerScript peer, ClientScript client) throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (ServerSocket listener = new ServerSocket(0)) {
            Future<?> peerResult = executor.submit(() -> {
                try (Socket socket = listener.accept()) {
                    socket.setSoTimeout(5_000);
                    peer.run(socket);
                }
                return null;
            });
            client.run(listener.getLocalPort());
            peerResult.get(5, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    private static void acceptHello(Socket socket) throws IOException {
        Frame hello = requireFrame(socket, MessageType.HELLO);
        write(socket, new Frame(
                MessageType.SUCCESS,
                hello.correlationId(),
                PayloadCodec.encodeSuccess(new PayloadCodec.Success("Connected"))));
    }

    private static Frame requireFrame(Socket socket, MessageType expected) throws IOException {
        Frame frame = FrameCodec.read(socket.getInputStream());
        assertNotNull(frame);
        assertEquals(expected, frame.type());
        return frame;
    }

    private static void writeError(
            Socket socket, long correlationId, ErrorCode code, String message) throws IOException {
        write(socket, new Frame(
                MessageType.ERROR,
                correlationId,
                PayloadCodec.encodeError(new PayloadCodec.Error(code, message))));
    }

    private static void write(Socket socket, Frame frame) throws IOException {
        FrameCodec.write(socket.getOutputStream(), frame);
        socket.getOutputStream().flush();
    }

    @FunctionalInterface
    private interface PeerScript {
        void run(Socket socket) throws Exception;
    }

    @FunctionalInterface
    private interface ClientScript {
        void run(int port) throws Exception;
    }
}
