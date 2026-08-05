package filewire.client;

import filewire.protocol.DigestUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AtomicDownloadTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void commitsOnlyAfterSizeAndDigestMatch() throws Exception {
        Path root = AtomicDownload.prepareRoot(temporaryDirectory.resolve("downloads"));
        byte[] content = new byte[] {0, 1, 2, 0, -1, 42};
        Path destination;

        try (AtomicDownload download = AtomicDownload.open(root, Path.of("result.bin"))) {
            download.write(content);
            destination = download.commit(content.length, digest(content));
        }

        assertArrayEquals(content, Files.readAllBytes(destination));
        assertTrue(destination.startsWith(root));
    }

    @Test
    void digestFailureRemovesTemporaryAndFinalFiles() throws Exception {
        Path root = AtomicDownload.prepareRoot(temporaryDirectory.resolve("downloads"));
        Path part;
        Path destination;

        try (AtomicDownload download = AtomicDownload.open(root, Path.of("bad.bin"))) {
            part = download.temporaryFile();
            destination = download.destination();
            download.write(new byte[] {1, 2, 3});
            assertThrows(IOException.class, () -> download.commit(3, digest(new byte[] {9, 9, 9})));
        }

        assertFalse(Files.exists(part));
        assertFalse(Files.exists(destination));
    }

    @Test
    void interruptionStyleCloseRemovesIncompletePart() throws Exception {
        Path root = AtomicDownload.prepareRoot(temporaryDirectory.resolve("downloads"));
        Path part;
        try (AtomicDownload download = AtomicDownload.open(root, Path.of("partial.bin"))) {
            part = download.temporaryFile();
            download.write(new byte[] {7, 8, 9});
            assertTrue(Files.exists(part));
        }
        assertFalse(Files.exists(part));
        assertFalse(Files.exists(root.resolve("partial.bin")));
    }

    @Test
    void rejectsTraversalAbsoluteOutsideAndExistingDestinations() throws Exception {
        Path root = AtomicDownload.prepareRoot(temporaryDirectory.resolve("downloads"));
        Path outside = temporaryDirectory.resolve("outside.bin").toAbsolutePath();
        Files.write(root.resolve("existing.bin"), new byte[] {4});

        assertThrows(IOException.class, () -> AtomicDownload.open(root, Path.of("..", "escape.bin")));
        assertThrows(IOException.class, () -> AtomicDownload.open(root, outside));
        assertThrows(IOException.class, () -> AtomicDownload.open(root, Path.of("existing.bin")));
        assertArrayEquals(new byte[] {4}, Files.readAllBytes(root.resolve("existing.bin")));
    }

    @Test
    void rejectsDirectoryTargetsAndSymlinkComponentsWhenSupported() throws Exception {
        Path root = AtomicDownload.prepareRoot(temporaryDirectory.resolve("downloads"));
        Files.createDirectory(root.resolve("directory"));
        assertThrows(IOException.class, () -> AtomicDownload.open(root, Path.of("directory")));

        Path outsideDirectory = Files.createDirectory(temporaryDirectory.resolve("outside"));
        Path link = root.resolve("link");
        try {
            Files.createSymbolicLink(link, outsideDirectory);
        } catch (IOException | UnsupportedOperationException unavailable) {
            return;
        }
        assertThrows(IOException.class, () -> AtomicDownload.open(root, Path.of("link", "file.bin")));
    }

    private static byte[] digest(byte[] content) {
        MessageDigest digest = DigestUtil.newSha256();
        return digest.digest(content);
    }
}
