package filewire.server;

import filewire.protocol.ErrorCode;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StorageSymlinkTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void doesNotFollowStoredSymlinksOutsideTheRoot() throws Exception {
        Path root = temporaryDirectory.resolve("storage");
        Path outside = temporaryDirectory.resolve("outside.bin");
        Files.write(outside, new byte[] {4, 2});

        try (StorageService storage = new StorageService(root, 1_000)) {
            Path link = root.resolve("link.bin");
            try {
                Files.createSymbolicLink(link, outside);
            } catch (IOException | UnsupportedOperationException | SecurityException exception) {
                Assumptions.abort("Symbolic links are not available to this test process: " + exception.getMessage());
            }

            RequestException downloadFailure = assertThrows(
                    RequestException.class,
                    () -> storage.openDownload("link.bin"));
            RequestException deleteFailure = assertThrows(
                    RequestException.class,
                    () -> storage.delete("link.bin"));

            assertEquals(ErrorCode.INVALID_FILENAME, downloadFailure.errorCode());
            assertEquals(ErrorCode.INVALID_FILENAME, deleteFailure.errorCode());
        }
    }
}
