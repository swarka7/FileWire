package filewire.server;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class ServerLifecycleRaceTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void concurrentStartAndCloseCannotLeaveAListenerOrSessionBehind() throws Exception {
        try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
            for (int iteration = 0; iteration < 24; iteration++) {
                FileWireServer server = new FileWireServer(new ServerConfig(
                        temporaryDirectory.resolve("storage-" + iteration),
                        0,
                        1,
                        1,
                        5_000,
                        1_000,
                        Duration.ofSeconds(1)));
                CountDownLatch start = new CountDownLatch(1);
                Future<Throwable> starter = executor.submit(() -> {
                    start.await();
                    try {
                        server.start();
                        return null;
                    } catch (Throwable failure) {
                        return failure;
                    }
                });
                Future<?> closer = executor.submit(() -> {
                    start.await();
                    server.close();
                    return null;
                });

                start.countDown();
                Throwable startFailure = starter.get(3, TimeUnit.SECONDS);
                closer.get(3, TimeUnit.SECONDS);
                server.close();

                if (startFailure != null) {
                    assertInstanceOf(IllegalStateException.class, startFailure);
                }
                assertFalse(server.isRunning());
                assertEquals(0, server.activeSessionCount());
            }
        }
    }
}
