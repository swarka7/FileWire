package filewire.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.List;

import org.junit.jupiter.api.Test;

class PayloadCodecTest {
    private static final byte[] DIGEST = digest();

    @Test
    void roundTripsDirectoryListWithUtf8Names() throws Exception {
        PayloadCodec.ListResponse expected = new PayloadCodec.ListResponse(
                List.of("alpha.txt", "résumé.bin", "שלום-世界.dat"));
        assertEquals(expected, PayloadCodec.decodeListResponse(
                PayloadCodec.encodeListResponse(expected)));
    }

    @Test
    void roundTripsUploadRequest() throws Exception {
        PayloadCodec.UploadRequest expected = new PayloadCodec.UploadRequest("upload.bin", 9_001, DIGEST);
        PayloadCodec.UploadRequest actual = PayloadCodec.decodeUploadRequest(
                PayloadCodec.encodeUploadRequest(expected));
        assertEquals(expected.filename(), actual.filename());
        assertEquals(expected.size(), actual.size());
        assertArrayEquals(expected.sha256(), actual.sha256());
    }

    @Test
    void roundTripsUploadAccepted() throws Exception {
        PayloadCodec.UploadAccepted expected = new PayloadCodec.UploadAccepted(19, 8192);
        assertEquals(expected, PayloadCodec.decodeUploadAccepted(
                PayloadCodec.encodeUploadAccepted(expected)));
    }

    @Test
    void roundTripsDownloadRequestAndDeleteRequest() throws Exception {
        PayloadCodec.DownloadRequest download = new PayloadCodec.DownloadRequest("data.bin");
        PayloadCodec.DeleteRequest delete = new PayloadCodec.DeleteRequest("old.bin");
        assertEquals(download, PayloadCodec.decodeDownloadRequest(
                PayloadCodec.encodeDownloadRequest(download)));
        assertEquals(delete, PayloadCodec.decodeDeleteRequest(
                PayloadCodec.encodeDeleteRequest(delete)));
    }

    @Test
    void roundTripsDownloadMetadata() throws Exception {
        PayloadCodec.DownloadMetadata expected = new PayloadCodec.DownloadMetadata(
                20, "data.bin", 123_456, DIGEST, ProtocolConstants.MAX_CHUNK_BYTES);
        PayloadCodec.DownloadMetadata actual = PayloadCodec.decodeDownloadMetadata(
                PayloadCodec.encodeDownloadMetadata(expected));
        assertEquals(expected.transferId(), actual.transferId());
        assertEquals(expected.filename(), actual.filename());
        assertEquals(expected.size(), actual.size());
        assertArrayEquals(expected.sha256(), actual.sha256());
        assertEquals(expected.chunkSize(), actual.chunkSize());
    }

    @Test
    void roundTripsEmptyAndFullFileChunks() throws Exception {
        PayloadCodec.FileChunk empty = new PayloadCodec.FileChunk(0, true, new byte[0]);
        byte[] fullData = new byte[ProtocolConstants.MAX_CHUNK_BYTES];
        for (int index = 0; index < fullData.length; index++) {
            fullData[index] = (byte) index;
        }
        PayloadCodec.FileChunk full = new PayloadCodec.FileChunk(17, true, fullData);

        PayloadCodec.FileChunk decodedEmpty = PayloadCodec.decodeFileChunk(
                PayloadCodec.encodeFileChunk(empty));
        PayloadCodec.FileChunk decodedFull = PayloadCodec.decodeFileChunk(
                PayloadCodec.encodeFileChunk(full));
        assertEquals(0, decodedEmpty.sequence());
        assertEquals(true, decodedEmpty.finalChunk());
        assertArrayEquals(new byte[0], decodedEmpty.data());
        assertEquals(17, decodedFull.sequence());
        assertArrayEquals(fullData, decodedFull.data());
    }

    @Test
    void roundTripsTransferCompleteSuccessAndError() throws Exception {
        PayloadCodec.TransferComplete complete = new PayloadCodec.TransferComplete(42, DIGEST);
        PayloadCodec.TransferComplete decodedComplete = PayloadCodec.decodeTransferComplete(
                PayloadCodec.encodeTransferComplete(complete));
        assertEquals(complete.totalBytes(), decodedComplete.totalBytes());
        assertArrayEquals(complete.sha256(), decodedComplete.sha256());

        PayloadCodec.Success success = new PayloadCodec.Success("stored");
        assertEquals(success, PayloadCodec.decodeSuccess(PayloadCodec.encodeSuccess(success)));

        PayloadCodec.Error error = new PayloadCodec.Error(ErrorCode.INTEGRITY_MISMATCH, "bad digest");
        assertEquals(error, PayloadCodec.decodeError(PayloadCodec.encodeError(error)));
    }

    @Test
    void rejectsInvalidUtf8() throws Exception {
        byte[] invalid = new byte[] {0, 2, (byte) 0xc3, 0x28};
        ProtocolException exception = assertThrows(
                ProtocolException.class,
                () -> PayloadCodec.decodeDownloadRequest(invalid));
        assertEquals(ErrorCode.MALFORMED_FRAME, exception.errorCode());
    }

    @Test
    void rejectsTrailingPayloadBytes() throws Exception {
        byte[] valid = PayloadCodec.encodeSuccess(new PayloadCodec.Success("ok"));
        byte[] trailing = java.util.Arrays.copyOf(valid, valid.length + 1);
        assertThrows(ProtocolException.class, () -> PayloadCodec.decodeSuccess(trailing));
    }

    @Test
    void rejectsNegativeFileSize() throws Exception {
        byte[] payload = raw(out -> {
            out.writeShort(5);
            out.writeBytes("a.bin");
            out.writeLong(-1);
            out.write(DIGEST);
        });
        assertThrows(ProtocolException.class, () -> PayloadCodec.decodeUploadRequest(payload));
    }

    @Test
    void rejectsInvalidChunkFinalFlag() throws Exception {
        byte[] payload = raw(out -> {
            out.writeInt(0);
            out.writeByte(2);
            out.writeInt(1);
            out.writeByte(7);
        });
        assertThrows(ProtocolException.class, () -> PayloadCodec.decodeFileChunk(payload));
    }

    @Test
    void rejectsNegativeChunkSequence() throws Exception {
        byte[] payload = raw(out -> {
            out.writeInt(-1);
            out.writeByte(1);
            out.writeInt(0);
        });
        assertThrows(ProtocolException.class, () -> PayloadCodec.decodeFileChunk(payload));
    }

    @Test
    void rejectsChunkLengthMismatch() throws Exception {
        byte[] payload = raw(out -> {
            out.writeInt(0);
            out.writeByte(1);
            out.writeInt(5);
            out.write(new byte[4]);
        });
        assertThrows(ProtocolException.class, () -> PayloadCodec.decodeFileChunk(payload));
    }

    @Test
    void rejectsEmptyNonFinalChunk() throws Exception {
        byte[] payload = raw(out -> {
            out.writeInt(0);
            out.writeByte(0);
            out.writeInt(0);
        });
        assertThrows(ProtocolException.class, () -> PayloadCodec.decodeFileChunk(payload));
    }

    @Test
    void rejectsUnknownErrorCode() throws Exception {
        byte[] payload = raw(out -> {
            out.writeShort(65535);
            out.writeShort(0);
        });
        assertThrows(ProtocolException.class, () -> PayloadCodec.decodeError(payload));
    }

    @Test
    void rejectsDirectoryListCountAboveLimitBeforeLooping() throws Exception {
        byte[] payload = raw(out -> out.writeInt(ProtocolConstants.MAX_LIST_ENTRIES + 1));
        assertThrows(ProtocolException.class, () -> PayloadCodec.decodeListResponse(payload));
    }

    @Test
    void payloadRecordsDefensivelyCopyMutableArrays() {
        byte[] digest = digest();
        PayloadCodec.UploadRequest request = new PayloadCodec.UploadRequest("safe.bin", 1, digest);
        digest[0] = 99;
        assertEquals(0, request.sha256()[0]);
        byte[] exposedDigest = request.sha256();
        exposedDigest[1] = 99;
        assertEquals(1, request.sha256()[1]);

        byte[] data = new byte[] {1, 2, 3};
        PayloadCodec.FileChunk chunk = new PayloadCodec.FileChunk(0, true, data);
        data[0] = 99;
        assertArrayEquals(new byte[] {1, 2, 3}, chunk.data());
    }

    @Test
    void frameDefensivelyCopiesPayload() {
        byte[] payload = new byte[0];
        Frame empty = new Frame(MessageType.HELLO, 1, payload);
        assertArrayEquals(new byte[0], empty.payload());

        byte[] successPayload;
        try {
            successPayload = PayloadCodec.encodeSuccess(new PayloadCodec.Success("ok"));
        } catch (ProtocolException exception) {
            throw new AssertionError(exception);
        }
        Frame frame = new Frame(MessageType.SUCCESS, 1, successPayload);
        successPayload[0] = 99;
        byte[] firstRead = frame.payload();
        firstRead[0] = 88;
        assertEquals(0, frame.payload()[0]);
    }

    private static byte[] digest() {
        byte[] digest = new byte[ProtocolConstants.SHA256_BYTES];
        for (int index = 0; index < digest.length; index++) {
            digest[index] = (byte) index;
        }
        return digest;
    }

    private static byte[] raw(Writer writer) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            writer.write(output);
        }
        return bytes.toByteArray();
    }

    @FunctionalInterface
    private interface Writer {
        void write(DataOutputStream output) throws Exception;
    }
}
