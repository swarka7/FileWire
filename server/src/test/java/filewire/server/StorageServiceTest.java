package filewire.server;

import filewire.protocol.ErrorCode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StorageServiceTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void acceptsPortableSingleComponentNamesAndListsOnlyRegularFiles() throws Exception {
        try (StorageService storage = storage()) {
            Path alpha = storage.resolveFile("alpha.bin");
            Path unicode = storage.resolveFile("שלום.dat");
            Files.write(alpha, new byte[] {0, 1, 0, 2});
            Files.write(unicode, "value".getBytes(StandardCharsets.UTF_8));
            Files.write(storage.root().resolve(StorageService.REPOSITORY_MARKER_NAME), new byte[0]);
            Files.createDirectory(storage.root().resolve("directory"));

            assertEquals(List.of("alpha.bin", "שלום.dat"), storage.listFiles());
            assertTrue(alpha.startsWith(storage.root()));
            assertEquals(storage.root(), alpha.getParent());
        }
    }

    @Test
    void repositoryMarkerIsNeverExposedAsAStoredFile() throws Exception {
        try (StorageService storage = storage()) {
            Files.write(storage.root().resolve(StorageService.REPOSITORY_MARKER_NAME), new byte[0]);

            RequestException failure = assertThrows(
                    RequestException.class,
                    () -> storage.resolveFile(StorageService.REPOSITORY_MARKER_NAME));
            RequestException caseVariantFailure = assertThrows(
                    RequestException.class,
                    () -> storage.resolveFile(".GITKEEP"));

            assertEquals(ErrorCode.INVALID_FILENAME, failure.errorCode());
            assertEquals(ErrorCode.INVALID_FILENAME, caseVariantFailure.errorCode());
            assertEquals(List.of(), storage.listFiles());
        }
    }

    @Test
    void rejectsTraversalAbsoluteAndMultiComponentNames() throws Exception {
        try (StorageService storage = storage()) {
            List<String> invalidNames = List.of(
                    " ",
                    ".",
                    "..",
                    "../outside.txt",
                    "../../outside.txt",
                    "/tmp/outside.txt",
                    "C:\\outside.txt",
                    "\\\\server\\share\\outside.txt",
                    "nested/file.txt",
                    "nested\\file.txt");

            for (String filename : invalidNames) {
                RequestException exception = assertThrows(
                        RequestException.class,
                        () -> storage.resolveFile(filename),
                        filename);
                assertEquals(ErrorCode.INVALID_FILENAME, exception.errorCode());
            }
        }
    }

    @Test
    void rejectsDirectoriesWhereRegularFilesAreRequired() throws Exception {
        try (StorageService storage = storage()) {
            Files.createDirectory(storage.root().resolve("folder"));

            RequestException downloadFailure = assertThrows(
                    RequestException.class,
                    () -> storage.openDownload("folder"));
            RequestException deleteFailure = assertThrows(
                    RequestException.class,
                    () -> storage.delete("folder"));

            assertEquals(ErrorCode.INVALID_FILENAME, downloadFailure.errorCode());
            assertEquals(ErrorCode.INVALID_FILENAME, deleteFailure.errorCode());
        }
    }

    @Test
    void startupAndCloseRemoveOnlyDedicatedTemporaryFiles() throws Exception {
        Path root = temporaryDirectory.resolve("storage");
        StorageService first = new StorageService(root, 1_000_000);
        Path incomplete = first.createTemporaryUpload(7);
        Files.write(incomplete, new byte[] {1, 2, 3});
        assertEquals(1, first.temporaryFileCount());
        first.close();
        assertEquals(0, countEntries(root.resolve(StorageService.TEMP_DIRECTORY_NAME)));

        Files.write(root.resolve("complete.bin"), new byte[] {9});
        try (StorageService second = new StorageService(root, 1_000_000)) {
            assertTrue(Files.exists(root.resolve("complete.bin")));
            assertEquals(0, second.temporaryFileCount());
        }
    }

    @Test
    void deleteDoesNotAcknowledgeMissingFiles() throws Exception {
        try (StorageService storage = storage()) {
            RequestException failure = assertThrows(
                    RequestException.class,
                    () -> storage.delete("missing.bin"));
            assertEquals(ErrorCode.FILE_NOT_FOUND, failure.errorCode());
            assertFalse(Files.exists(storage.root().resolve("missing.bin")));
        }
    }

    private StorageService storage() throws IOException {
        return new StorageService(temporaryDirectory.resolve("storage"), 1_000_000);
    }

    private static int countEntries(Path directory) throws IOException {
        try (var entries = Files.list(directory)) {
            return Math.toIntExact(entries.count());
        }
    }
}
