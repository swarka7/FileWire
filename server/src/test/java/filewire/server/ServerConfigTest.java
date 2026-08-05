package filewire.server;

import filewire.protocol.ProtocolConstants;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ServerConfigTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void shutdownTimeoutMustBeRepresentableAndAtLeastOneMillisecond() {
        assertThrows(IllegalArgumentException.class, () -> config(Duration.ofNanos(1)));
        assertThrows(IllegalArgumentException.class, () -> config(Duration.ofSeconds(Long.MAX_VALUE)));
        assertDoesNotThrow(() -> config(Duration.ofMillis(1)));
    }

    @Test
    void readTimeoutMustLeaveMarginBeyondPreparationKeepaliveInterval() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new ServerConfig(
                        temporaryDirectory,
                        0,
                        1,
                        1,
                        ProtocolConstants.PREPARATION_KEEPALIVE_INTERVAL_MILLIS,
                        1_000,
                        Duration.ofSeconds(1)));
        assertDoesNotThrow(() -> new ServerConfig(
                temporaryDirectory,
                0,
                1,
                1,
                ProtocolConstants.PREPARATION_KEEPALIVE_INTERVAL_MILLIS * 2,
                1_000,
                Duration.ofSeconds(1)));
    }

    private ServerConfig config(Duration timeout) {
        return new ServerConfig(temporaryDirectory, 0, 1, 1, 5_000, 1_000, timeout);
    }
}
