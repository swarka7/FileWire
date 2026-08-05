package filewire.server;

import filewire.protocol.DigestUtil;
import filewire.protocol.ErrorCode;
import filewire.protocol.ProtocolConstants;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class UploadTransactionTest {
    @TempDir
    Path temporaryDirectory;

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 65_535, 65_536, 65_537, 131_072, 150_001})
    void streamsBoundarySizesWithoutBufferingTheWholeTransfer(int size) throws Exception {
        byte[] expected = randomBytes(size);
        try (TransferService transfers = service()) {
            UploadTransaction upload = transfers.beginUpload(1, "boundary.bin", size, digest(expected));
            stream(upload, expected);

            assertArrayEquals(expected, Files.readAllBytes(temporaryDirectory.resolve("storage/boundary.bin")));
            assertEquals(0, transfers.activeTransferCount());
            assertEquals(0, transfers.reservationCount());
            assertEquals(0, countTemporaryFiles());
        }
    }

    @Test
    void incorrectDigestDeletesTemporaryAndCompletedFiles() throws Exception {
        byte[] data = randomBytes(ProtocolConstants.MAX_CHUNK_BYTES + 7);
        byte[] incorrectDigest = digest(data);
        incorrectDigest[0] ^= 0x55;

        try (TransferService transfers = service()) {
            UploadTransaction upload = transfers.beginUpload(7, "corrupt.bin", data.length, incorrectDigest);
            upload.receive(0, false, Arrays.copyOfRange(data, 0, ProtocolConstants.MAX_CHUNK_BYTES));
            RequestException failure = assertThrows(
                    RequestException.class,
                    () -> upload.receive(
                            1,
                            true,
                            Arrays.copyOfRange(data, ProtocolConstants.MAX_CHUNK_BYTES, data.length)));

            assertEquals(ErrorCode.INTEGRITY_MISMATCH, failure.errorCode());
            assertFalse(Files.exists(temporaryDirectory.resolve("storage/corrupt.bin")));
            assertEquals(0, transfers.activeTransferCount());
            assertEquals(0, transfers.reservationCount());
            assertEquals(0, countTemporaryFiles());
        }
    }

    @Test
    void sizeAndSequenceViolationsAbortTheUpload() throws Exception {
        byte[] data = randomBytes(10);
        try (TransferService transfers = service()) {
            UploadTransaction wrongSequence = transfers.beginUpload(1, "sequence.bin", 10, digest(data));
            RequestException sequenceFailure = assertThrows(
                    RequestException.class,
                    () -> wrongSequence.receive(1, true, data));
            assertEquals(ErrorCode.INVALID_REQUEST, sequenceFailure.errorCode());

            UploadTransaction wrongSize = transfers.beginUpload(1, "size.bin", 11, digest(data));
            RequestException sizeFailure = assertThrows(
                    RequestException.class,
                    () -> wrongSize.receive(0, true, data));
            assertEquals(ErrorCode.INVALID_REQUEST, sizeFailure.errorCode());

            assertEquals(0, transfers.activeTransferCount());
            assertEquals(0, transfers.reservationCount());
            assertEquals(0, countTemporaryFiles());
        }
    }

    @Test
    void conflictingUploadsAreRejectedAndReservationIsReleasedAfterInterruption() throws Exception {
        byte[] data = randomBytes(100);
        try (TransferService transfers = service()) {
            UploadTransaction first = transfers.beginUpload(10, "claimed.bin", data.length, digest(data));
            first.receive(0, false, Arrays.copyOf(data, 40));

            RequestException conflict = assertThrows(
                    RequestException.class,
                    () -> transfers.beginUpload(11, "claimed.bin", data.length, digest(data)));
            assertEquals(ErrorCode.TRANSFER_CONFLICT, conflict.errorCode());

            transfers.abortSession(10);
            assertEquals(0, transfers.reservationCount());
            assertEquals(0, countTemporaryFiles());

            UploadTransaction retry = transfers.beginUpload(11, "claimed.bin", data.length, digest(data));
            stream(retry, data);
            assertArrayEquals(data, Files.readAllBytes(temporaryDirectory.resolve("storage/claimed.bin")));
        }
    }

    @Test
    void existingDestinationIsNeverOverwritten() throws Exception {
        Path root = temporaryDirectory.resolve("storage");
        Files.createDirectories(root);
        Files.write(root.resolve("existing.bin"), new byte[] {9, 8, 7});

        try (TransferService transfers = service()) {
            RequestException failure = assertThrows(
                    RequestException.class,
                    () -> transfers.beginUpload(1, "existing.bin", 1, digest(new byte[] {1})));
            assertEquals(ErrorCode.FILE_ALREADY_EXISTS, failure.errorCode());
            assertArrayEquals(new byte[] {9, 8, 7}, Files.readAllBytes(root.resolve("existing.bin")));
            assertEquals(0, transfers.reservationCount());
        }
    }

    private TransferService service() throws Exception {
        return new TransferService(new StorageService(temporaryDirectory.resolve("storage"), 10_000_000));
    }

    private void stream(UploadTransaction upload, byte[] data) throws Exception {
        if (data.length == 0) {
            upload.receive(0, true, new byte[0]);
            return;
        }
        int offset = 0;
        int sequence = 0;
        while (offset < data.length) {
            int end = Math.min(data.length, offset + ProtocolConstants.MAX_CHUNK_BYTES);
            upload.receive(sequence++, end == data.length, Arrays.copyOfRange(data, offset, end));
            offset = end;
        }
    }

    private byte[] digest(byte[] data) throws Exception {
        return DigestUtil.sha256(new ByteArrayInputStream(data));
    }

    private byte[] randomBytes(int size) {
        byte[] data = new byte[size];
        new Random(7_331L + size).nextBytes(data);
        return data;
    }

    private int countTemporaryFiles() throws Exception {
        Path temp = temporaryDirectory.resolve("storage").resolve(StorageService.TEMP_DIRECTORY_NAME);
        try (var entries = Files.list(temp)) {
            return Math.toIntExact(entries.count());
        }
    }
}
