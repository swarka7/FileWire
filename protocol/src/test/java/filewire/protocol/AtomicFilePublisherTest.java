package filewire.protocol;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AtomicFilePublisherTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void publishesCompletedFileAndRemovesStagingName() throws Exception {
        byte[] content = {0, 1, 2, -1, 42};
        Path source = Files.write(temporaryDirectory.resolve("upload.part"), content);
        Path target = temporaryDirectory.resolve("result.bin");

        AtomicFilePublisher.publishCreateOnly(source, target);

        assertFalse(Files.exists(source));
        assertArrayEquals(content, Files.readAllBytes(target));
    }

    @Test
    void neverReplacesAnExistingTarget() throws Exception {
        byte[] existing = {9, 8, 7};
        Path source = Files.write(temporaryDirectory.resolve("upload.part"), new byte[] {1, 2, 3});
        Path target = Files.write(temporaryDirectory.resolve("result.bin"), existing);

        assertThrows(
                FileAlreadyExistsException.class,
                () -> AtomicFilePublisher.publishCreateOnly(source, target));

        assertArrayEquals(existing, Files.readAllBytes(target));
        assertTrue(Files.exists(source));
    }

    @Test
    void concurrentPublishersProduceExactlyOneWinnerWithoutOverwrite() throws Exception {
        int publisherCount = 8;
        Path target = temporaryDirectory.resolve("winner.bin");
        List<Path> sources = new ArrayList<>();
        for (int index = 0; index < publisherCount; index++) {
            sources.add(Files.write(
                    temporaryDirectory.resolve("source-" + index + ".part"),
                    new byte[] {(byte) index}));
        }

        CountDownLatch ready = new CountDownLatch(publisherCount);
        CountDownLatch start = new CountDownLatch(1);
        try (ExecutorService executor = Executors.newFixedThreadPool(publisherCount)) {
            List<Future<Boolean>> results = new ArrayList<>();
            for (Path source : sources) {
                results.add(executor.submit(() -> {
                    ready.countDown();
                    start.await();
                    try {
                        AtomicFilePublisher.publishCreateOnly(source, target);
                        return true;
                    } catch (FileAlreadyExistsException collision) {
                        return false;
                    }
                }));
            }
            ready.await();
            start.countDown();

            int winners = 0;
            for (Future<Boolean> result : results) {
                if (result.get()) {
                    winners++;
                }
            }
            assertEquals(1, winners);
        }

        byte[] published = Files.readAllBytes(target);
        assertEquals(1, published.length);
        int value = Byte.toUnsignedInt(published[0]);
        assertTrue(value < publisherCount);
        assertFalse(Files.exists(sources.get(value)));
        for (int index = 0; index < publisherCount; index++) {
            if (index != value) {
                assertTrue(Files.exists(sources.get(index)));
            }
        }
    }
}
