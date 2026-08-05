package filewire.client;

import filewire.protocol.Frame;
import filewire.protocol.FrameCodec;
import filewire.protocol.MessageType;
import filewire.protocol.PayloadCodec;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FileWireCloseCancellationTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void closeImmediatelyCancelsAnOperationBlockedInSocketIo() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch listReceived = new CountDownLatch(1);
        FileWireClient client = null;
        try (ServerSocket listener = new ServerSocket(0)) {
            Future<?> peer = executor.submit(() -> {
                try (Socket socket = listener.accept()) {
                    socket.setSoTimeout(5_000);
                    Frame hello = requireFrame(socket, MessageType.HELLO);
                    write(socket, new Frame(
                            MessageType.SUCCESS,
                            hello.correlationId(),
                            PayloadCodec.encodeSuccess(new PayloadCodec.Success("Connected"))));
                    requireFrame(socket, MessageType.LIST_REQUEST);
                    listReceived.countDown();
                    assertEquals(-1, socket.getInputStream().read());
                }
                return null;
            });

            client = FileWireClient.connect(
                    "127.0.0.1", listener.getLocalPort(), temporaryDirectory.resolve("downloads"));
            FileWireClient connectedClient = client;
            Future<?> blockedList = executor.submit(connectedClient::listFiles);
            assertTrue(listReceived.await(5, TimeUnit.SECONDS));

            assertTimeoutPreemptively(Duration.ofSeconds(2), connectedClient::close);
            assertFalse(connectedClient.isConnected());
            ExecutionException operationFailure = assertThrows(
                    ExecutionException.class,
                    () -> blockedList.get(5, TimeUnit.SECONDS));
            assertInstanceOf(IOException.class, operationFailure.getCause());
            peer.get(5, TimeUnit.SECONDS);
        } finally {
            if (client != null) {
                client.close();
            }
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    private static Frame requireFrame(Socket socket, MessageType expected) throws IOException {
        Frame frame = FrameCodec.read(socket.getInputStream());
        assertNotNull(frame);
        assertEquals(expected, frame.type());
        return frame;
    }

    private static void write(Socket socket, Frame frame) throws IOException {
        FrameCodec.write(socket.getOutputStream(), frame);
        socket.getOutputStream().flush();
    }
}
