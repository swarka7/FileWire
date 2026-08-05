package filewire.server;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AtomicIdGeneratorTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void allocatesUniquePositiveIdsConcurrently() throws Exception {
        AtomicIdGenerator generator = new AtomicIdGenerator();
        ExecutorService executor = Executors.newFixedThreadPool(12);
        try {
            List<Callable<Long>> allocations = new ArrayList<>();
            for (int index = 0; index < 20_000; index++) {
                allocations.add(generator::nextId);
            }
            List<Future<Long>> futures = executor.invokeAll(allocations);
            Set<Long> ids = new HashSet<>();
            for (Future<Long> future : futures) {
                long id = future.get();
                assertTrue(id > 0);
                ids.add(id);
            }
            assertEquals(allocations.size(), ids.size());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void transferIdsUseTheSameAtomicUniquenessGuarantee() throws Exception {
        try (TransferService transfers = new TransferService(
                new StorageService(temporaryDirectory.resolve("storage"), 100))) {
            ExecutorService executor = Executors.newFixedThreadPool(8);
            try {
                List<Callable<Long>> allocations = new ArrayList<>();
                for (int index = 0; index < 5_000; index++) {
                    allocations.add(transfers::nextTransferIdForTest);
                }
                Set<Long> ids = new HashSet<>();
                for (Future<Long> future : executor.invokeAll(allocations)) {
                    ids.add(future.get());
                }
                assertEquals(allocations.size(), ids.size());
            } finally {
                executor.shutdownNow();
            }
        }
    }
}
