package filewire.client;

import filewire.protocol.ErrorCode;
import filewire.protocol.ProtocolConstants;
import filewire.server.FileWireServer;
import filewire.server.ServerConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class FileWireConcurrencyIntegrationTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void concurrentClientsResolveConflictsTransferDifferentFilesAndCleanRegistry() throws Exception {
        Path storage = temporaryDirectory.resolve("server-files");
        Path sources = Files.createDirectory(temporaryDirectory.resolve("sources"));
        Path firstRoot = temporaryDirectory.resolve("downloads-one");
        Path secondRoot = temporaryDirectory.resolve("downloads-two");
        Path firstContender = writeRandom(sources.resolve("first-contender.bin"), 2_000_003, 11);
        Path secondContender = writeRandom(sources.resolve("second-contender.bin"), 2_000_003, 12);
        byte[] firstUniqueBytes = randomBytes(ProtocolConstants.MAX_CHUNK_BYTES * 80 + 137, 21);
        byte[] secondUniqueBytes = randomBytes(ProtocolConstants.MAX_CHUNK_BYTES * 5 + 23, 22);
        Path firstUnique = sources.resolve("first-unique.bin");
        Path secondUnique = sources.resolve("second-unique.bin");
        Files.write(firstUnique, firstUniqueBytes);
        Files.write(secondUnique, secondUniqueBytes);

        FileWireServer server = new FileWireServer(ServerConfig.defaults(storage, 0));
        ExecutorService executor = Executors.newFixedThreadPool(2);
        server.start();
        try {
            try (FileWireClient first = FileWireClient.connect("127.0.0.1", server.port(), firstRoot);
                 FileWireClient second = FileWireClient.connect("127.0.0.1", server.port(), secondRoot)) {
                CountDownLatch raceReady = new CountDownLatch(2);
                CountDownLatch raceStart = new CountDownLatch(1);
                Future<RaceResult> firstRace = executor.submit(
                        () -> raceUpload(first, firstContender, raceReady, raceStart));
                Future<RaceResult> secondRace = executor.submit(
                        () -> raceUpload(second, secondContender, raceReady, raceStart));
                assertTrue(raceReady.await(5, TimeUnit.SECONDS), "clients did not reach the race gate");
                raceStart.countDown();

                RaceResult firstResult = firstRace.get(15, TimeUnit.SECONDS);
                RaceResult secondResult = secondRace.get(15, TimeUnit.SECONDS);
                assertEquals(1, (firstResult.succeeded() ? 1 : 0) + (secondResult.succeeded() ? 1 : 0));
                RaceResult loser = firstResult.succeeded() ? secondResult : firstResult;
                assertTrue(
                        Set.of(ErrorCode.TRANSFER_CONFLICT, ErrorCode.FILE_ALREADY_EXISTS)
                                .contains(loser.errorCode()),
                        "unexpected conflict error: " + loser.errorCode());

                assertThrows(
                        IOException.class,
                        () -> first.download("contested.bin", Path.of("..", "outside-download.bin")));
                assertFalse(Files.exists(temporaryDirectory.resolve("outside-download.bin")));
                assertTrue(first.isConnected());

                CountDownLatch transferReady = new CountDownLatch(2);
                CountDownLatch transferStart = new CountDownLatch(1);
                Future<byte[]> firstTransfer = executor.submit(() -> uploadAndDownload(
                        first, firstUnique, "alpha.bin", Path.of("alpha-copy.bin"), firstRoot,
                        transferReady, transferStart));
                Future<byte[]> secondTransfer = executor.submit(() -> uploadAndDownload(
                        second, secondUnique, "beta.bin", Path.of("beta-copy.bin"), secondRoot,
                        transferReady, transferStart));
                assertTrue(transferReady.await(5, TimeUnit.SECONDS), "clients did not reach the transfer gate");
                transferStart.countDown();

                assertArrayEquals(firstUniqueBytes, firstTransfer.get(15, TimeUnit.SECONDS));
                assertArrayEquals(secondUniqueBytes, secondTransfer.get(15, TimeUnit.SECONDS));
                assertEquals(Set.of("contested.bin", "alpha.bin", "beta.bin"), Set.copyOf(first.listFiles()));

                first.delete("contested.bin");
                first.delete("alpha.bin");
                second.delete("beta.bin");
                first.disconnect();
                second.disconnect();
                assertFalse(first.isConnected());
                assertFalse(second.isConnected());

                assertTimeoutPreemptively(Duration.ofSeconds(2), () -> {
                    while (server.activeSessionCount() != 0) {
                        Thread.onSpinWait();
                    }
                });
            }
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
            server.close();
        }

        assertEquals(0, server.activeSessionCount());
        assertDirectoryEmpty(storage.resolve(".filewire-tmp"));
    }

    private static RaceResult raceUpload(
            FileWireClient client,
            Path source,
            CountDownLatch ready,
            CountDownLatch start) throws Exception {
        ready.countDown();
        if (!start.await(5, TimeUnit.SECONDS)) {
            fail("race gate was not released");
        }
        try {
            client.upload(source, "contested.bin");
            return new RaceResult(true, null);
        } catch (RemoteOperationException conflict) {
            return new RaceResult(false, conflict.errorCode());
        }
    }

    private static byte[] uploadAndDownload(
            FileWireClient client,
            Path source,
            String remoteName,
            Path destination,
            Path downloadRoot,
            CountDownLatch ready,
            CountDownLatch start) throws Exception {
        ready.countDown();
        if (!start.await(5, TimeUnit.SECONDS)) {
            fail("transfer gate was not released");
        }
        client.upload(source, remoteName);
        client.download(remoteName, destination);
        return Files.readAllBytes(downloadRoot.resolve(destination));
    }

    private static Path writeRandom(Path path, int length, long seed) throws Exception {
        Files.write(path, randomBytes(length, seed));
        return path;
    }

    private static byte[] randomBytes(int length, long seed) {
        byte[] bytes = new byte[length];
        new Random(seed).nextBytes(bytes);
        return bytes;
    }

    private static void assertDirectoryEmpty(Path directory) throws Exception {
        if (!Files.exists(directory)) {
            return;
        }
        try (Stream<Path> entries = Files.list(directory)) {
            assertTrue(entries.findAny().isEmpty(), "temporary upload directory is not empty");
        }
    }

    private record RaceResult(boolean succeeded, ErrorCode errorCode) {
    }
}
