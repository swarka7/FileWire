package filewire.server;

import filewire.protocol.ErrorCode;
import filewire.protocol.Frame;
import filewire.protocol.FrameCodec;
import filewire.protocol.MessageType;
import filewire.protocol.PayloadCodec;
import filewire.protocol.ProtocolConstants;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.DataOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.file.Path;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class MalformedSessionTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void replySafeMalformedLengthGetsStructuredErrorThenConnectionCloses() throws Exception {
        ServerConfig config = new ServerConfig(
                temporaryDirectory.resolve("storage"),
                0,
                1,
                1,
                5_000,
                1_000_000,
                Duration.ofSeconds(2));
        try (FileWireServer server = new FileWireServer(config)) {
            server.start();
            try (Socket socket = new Socket(InetAddress.getLoopbackAddress(), server.port())) {
                socket.setSoTimeout(3_000);
                DataOutputStream output = new DataOutputStream(socket.getOutputStream());
                output.writeInt(ProtocolConstants.MAGIC);
                output.writeByte(ProtocolConstants.VERSION);
                output.writeByte(MessageType.LIST_REQUEST.code());
                output.writeLong(41);
                output.writeInt(-1);
                output.flush();

                Frame response = FrameCodec.read(socket.getInputStream());
                assertNotNull(response);
                assertEquals(MessageType.ERROR, response.type());
                assertEquals(41, response.correlationId());
                assertEquals(ErrorCode.MALFORMED_FRAME, PayloadCodec.decodeError(response.payload()).code());
                assertNull(FrameCodec.read(socket.getInputStream()));
            }
        }
    }
}
