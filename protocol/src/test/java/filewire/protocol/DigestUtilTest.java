package filewire.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DigestUtilTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void hashesPathWithoutLoadingWholeFile() throws Exception {
        Path file = temporaryDirectory.resolve("data.bin");
        Files.write(file, "abc".getBytes(StandardCharsets.UTF_8));
        assertEquals(
                "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
                DigestUtil.toHex(DigestUtil.sha256(file)));
    }

    @Test
    void hashesButDoesNotCloseCallerOwnedStream() throws Exception {
        TrackingInputStream input = new TrackingInputStream(new byte[] {0, 1, 0, 2});
        byte[] digest = DigestUtil.sha256(input);
        assertEquals(ProtocolConstants.SHA256_BYTES, digest.length);
        assertFalse(input.closed);
    }

    @Test
    void comparesDigestsWithoutEarlyExitApi() {
        byte[] first = DigestUtil.newSha256().digest(new byte[] {1});
        byte[] same = first.clone();
        byte[] different = DigestUtil.newSha256().digest(new byte[] {2});
        assertTrue(DigestUtil.matches(first, same));
        assertFalse(DigestUtil.matches(first, different));
    }

    private static final class TrackingInputStream extends ByteArrayInputStream {
        private boolean closed;

        private TrackingInputStream(byte[] bytes) {
            super(bytes);
        }

        @Override
        public void close() throws IOException {
            closed = true;
            super.close();
        }
    }
}
