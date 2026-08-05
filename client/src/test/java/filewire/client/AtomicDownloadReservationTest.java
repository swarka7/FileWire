package filewire.client;

import filewire.protocol.DigestUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AtomicDownloadReservationTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void sizeMismatchCleansPartAndReleasesReservationIdempotently() throws Exception {
        Path root = AtomicDownload.prepareRoot(temporaryDirectory.resolve("downloads"));
        Path destination = Path.of("size-mismatch.bin");
        byte[] content = {1, 2, 3};
        AtomicDownload failed = AtomicDownload.open(root, destination);
        Path part = failed.temporaryFile();
        failed.write(content);

        assertThrows(IOException.class, () -> failed.commit(content.length + 1, digest(content)));
        assertFalse(Files.exists(part));
        assertFalse(Files.exists(root.resolve(destination)));

        AtomicDownload replacement = AtomicDownload.open(root, destination);
        failed.close();
        assertThrows(IOException.class, () -> AtomicDownload.open(root, destination));
        replacement.close();

        try (AtomicDownload reopened = AtomicDownload.open(root, destination)) {
            assertNotNull(reopened.temporaryFile());
        }
    }

    @Test
    void simultaneousOpenAllowsExactlyOneReservationAndCloseReleasesIt() throws Exception {
        Path root = AtomicDownload.prepareRoot(temporaryDirectory.resolve("downloads"));
        Path destination = Path.of("shared.bin");
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);
        try {
            Future<OpenAttempt> first = executor.submit(() -> attemptOpen(root, destination, ready, start));
            Future<OpenAttempt> second = executor.submit(() -> attemptOpen(root, destination, ready, start));
            assertTrue(ready.await(5, TimeUnit.SECONDS));
            start.countDown();

            OpenAttempt firstAttempt = first.get(5, TimeUnit.SECONDS);
            OpenAttempt secondAttempt = second.get(5, TimeUnit.SECONDS);
            assertEquals(
                    1,
                    (firstAttempt.download() == null ? 0 : 1)
                            + (secondAttempt.download() == null ? 0 : 1));
            OpenAttempt winner = firstAttempt.download() != null ? firstAttempt : secondAttempt;
            OpenAttempt loser = firstAttempt.download() == null ? firstAttempt : secondAttempt;
            assertNull(winner.failure());
            assertNotNull(loser.failure());
            assertThrows(IOException.class, () -> AtomicDownload.open(root, destination));

            winner.download().close();
            winner.download().close();
            try (AtomicDownload reopened = AtomicDownload.open(root, destination)) {
                assertNotNull(reopened.temporaryFile());
            }
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void successfulCommitReleasesReservationBeforeCloseWithoutReleasingANewerOwner() throws Exception {
        Path root = AtomicDownload.prepareRoot(temporaryDirectory.resolve("downloads"));
        Path relativeDestination = Path.of("committed.bin");
        Path destination = root.resolve(relativeDestination);
        byte[] content = {9, 8, 7};
        AtomicDownload committed = AtomicDownload.open(root, relativeDestination);
        committed.write(content);
        committed.commit(content.length, digest(content));

        Files.delete(destination);
        AtomicDownload replacement = AtomicDownload.open(root, relativeDestination);
        committed.close();
        assertThrows(IOException.class, () -> AtomicDownload.open(root, relativeDestination));
        replacement.close();
    }

    private static OpenAttempt attemptOpen(
            Path root,
            Path destination,
            CountDownLatch ready,
            CountDownLatch start) throws Exception {
        ready.countDown();
        if (!start.await(5, TimeUnit.SECONDS)) {
            throw new IllegalStateException("reservation race was not released");
        }
        try {
            return new OpenAttempt(AtomicDownload.open(root, destination), null);
        } catch (IOException failure) {
            return new OpenAttempt(null, failure);
        }
    }

    private static byte[] digest(byte[] content) {
        MessageDigest digest = DigestUtil.newSha256();
        return digest.digest(content);
    }

    private record OpenAttempt(AtomicDownload download, IOException failure) {
    }
}
