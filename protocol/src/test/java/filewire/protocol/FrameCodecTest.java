package filewire.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FrameCodecTest {
    private static final byte[] DIGEST = new byte[ProtocolConstants.SHA256_BYTES];

    @Test
    void roundTripsEveryMessageType() throws Exception {
        byte[] binary = new byte[512];
        for (int index = 0; index < binary.length; index++) {
            binary[index] = (byte) index;
        }

        List<Frame> frames = List.of(
                frame(MessageType.HELLO, new byte[0]),
                frame(MessageType.LIST_REQUEST, new byte[0]),
                frame(MessageType.LIST_RESPONSE, PayloadCodec.encodeListResponse(
                        new PayloadCodec.ListResponse(List.of("plain.txt", "נתונים-世界.bin")))),
                frame(MessageType.UPLOAD_REQUEST, PayloadCodec.encodeUploadRequest(
                        new PayloadCodec.UploadRequest("upload.bin", 512, DIGEST))),
                frame(MessageType.UPLOAD_ACCEPTED, PayloadCodec.encodeUploadAccepted(
                        new PayloadCodec.UploadAccepted(91, ProtocolConstants.MAX_CHUNK_BYTES))),
                frame(MessageType.DOWNLOAD_REQUEST, PayloadCodec.encodeDownloadRequest(
                        new PayloadCodec.DownloadRequest("download.bin"))),
                frame(MessageType.DOWNLOAD_METADATA, PayloadCodec.encodeDownloadMetadata(
                        new PayloadCodec.DownloadMetadata(92, "download.bin", 512, DIGEST, 4096))),
                frame(MessageType.FILE_CHUNK, PayloadCodec.encodeFileChunk(
                        new PayloadCodec.FileChunk(0, true, binary))),
                frame(MessageType.TRANSFER_COMPLETE, PayloadCodec.encodeTransferComplete(
                        new PayloadCodec.TransferComplete(512, DIGEST))),
                frame(MessageType.DELETE_REQUEST, PayloadCodec.encodeDeleteRequest(
                        new PayloadCodec.DeleteRequest("old.bin"))),
                frame(MessageType.SUCCESS, PayloadCodec.encodeSuccess(
                        new PayloadCodec.Success("complete"))),
                frame(MessageType.ERROR, PayloadCodec.encodeError(
                        new PayloadCodec.Error(ErrorCode.FILE_NOT_FOUND, "missing"))),
                frame(MessageType.DISCONNECT, new byte[0]),
                frame(MessageType.KEEPALIVE, new byte[0]));

        for (Frame expected : frames) {
            Frame actual = FrameCodec.decode(FrameCodec.encode(expected));
            assertFrameEquals(expected, actual);
        }
    }

    @Test
    void readsInputFragmentedOneByteAtATime() throws Exception {
        Frame expected = frame(MessageType.SUCCESS,
                PayloadCodec.encodeSuccess(new PayloadCodec.Success("fragmented")));
        InputStream fragmented = new OneByteAtATimeInputStream(
                new ByteArrayInputStream(FrameCodec.encode(expected)));

        assertFrameEquals(expected, FrameCodec.read(fragmented));
        assertNull(FrameCodec.read(fragmented));
    }

    @Test
    void readsMultipleFramesFromOneStream() throws Exception {
        Frame first = frame(MessageType.HELLO, new byte[0]);
        Frame second = frame(MessageType.LIST_REQUEST, new byte[0]);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        FrameCodec.write(bytes, first);
        FrameCodec.write(bytes, second);

        ByteArrayInputStream input = new ByteArrayInputStream(bytes.toByteArray());
        assertFrameEquals(first, FrameCodec.read(input));
        assertFrameEquals(second, FrameCodec.read(input));
        assertNull(FrameCodec.read(input));
    }

    @Test
    void preservesArbitraryBinaryDataIncludingZeroBytes() throws Exception {
        byte[] data = new byte[256];
        for (int index = 0; index < data.length; index++) {
            data[index] = (byte) index;
        }
        Frame frame = frame(MessageType.FILE_CHUNK,
                PayloadCodec.encodeFileChunk(new PayloadCodec.FileChunk(7, true, data)));

        PayloadCodec.FileChunk decoded = PayloadCodec.decodeFileChunk(
                FrameCodec.decode(FrameCodec.encode(frame)).payload());
        assertArrayEquals(data, decoded.data());
    }

    @Test
    void cleanEofReturnsNull() throws Exception {
        assertNull(FrameCodec.read(new ByteArrayInputStream(new byte[0])));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 5, 17})
    void rejectsTruncatedHeaders(int byteCount) {
        byte[] partial = new byte[byteCount];
        ProtocolException exception = assertThrows(
                ProtocolException.class,
                () -> FrameCodec.read(new ByteArrayInputStream(partial)));
        assertEquals(ErrorCode.MALFORMED_FRAME, exception.errorCode());
        assertFalse(exception.recoverable());
    }

    @Test
    void rejectsTruncatedPayload() throws Exception {
        byte[] encoded = FrameCodec.encode(frame(
                MessageType.SUCCESS,
                PayloadCodec.encodeSuccess(new PayloadCodec.Success("hello"))));
        byte[] truncated = java.util.Arrays.copyOf(encoded, encoded.length - 1);

        ProtocolException exception = assertThrows(
                ProtocolException.class,
                () -> FrameCodec.read(new ByteArrayInputStream(truncated)));
        assertEquals(ErrorCode.MALFORMED_FRAME, exception.errorCode());
        assertFalse(exception.recoverable());
    }

    @Test
    void rejectsInvalidMagic() throws Exception {
        byte[] bytes = rawFrame(0x0102_0304, ProtocolConstants.VERSION,
                MessageType.HELLO.code(), 1, 0, new byte[0]);
        ProtocolException exception = assertThrows(ProtocolException.class, () -> FrameCodec.decode(bytes));
        assertEquals(ErrorCode.MALFORMED_FRAME, exception.errorCode());
        assertFalse(exception.replySafe());
    }

    @Test
    void rejectsUnsupportedVersion() throws Exception {
        byte[] bytes = rawFrame(ProtocolConstants.MAGIC, ProtocolConstants.VERSION + 1,
                MessageType.HELLO.code(), 1, 0, new byte[0]);
        ProtocolException exception = assertThrows(ProtocolException.class, () -> FrameCodec.decode(bytes));
        assertEquals(ErrorCode.UNSUPPORTED_VERSION, exception.errorCode());
    }

    @Test
    void rejectsUnknownMessageType() throws Exception {
        byte[] bytes = rawFrame(ProtocolConstants.MAGIC, ProtocolConstants.VERSION,
                0x7f, 44, 0, new byte[0]);
        ProtocolException exception = assertThrows(ProtocolException.class, () -> FrameCodec.decode(bytes));
        assertEquals(ErrorCode.UNSUPPORTED_MESSAGE, exception.errorCode());
        assertEquals(44, exception.correlationId());
        assertFalse(exception.recoverable());
    }

    @Test
    void rejectsNonPositiveCorrelationId() throws Exception {
        byte[] bytes = rawFrame(ProtocolConstants.MAGIC, ProtocolConstants.VERSION,
                MessageType.HELLO.code(), 0, 0, new byte[0]);
        ProtocolException exception = assertThrows(ProtocolException.class, () -> FrameCodec.decode(bytes));
        assertEquals(ErrorCode.INVALID_REQUEST, exception.errorCode());
        assertFalse(exception.replySafe());
    }

    @Test
    void rejectsNegativePayloadLengthBeforeAllocation() throws Exception {
        byte[] bytes = rawFrame(ProtocolConstants.MAGIC, ProtocolConstants.VERSION,
                MessageType.SUCCESS.code(), 5, -1, new byte[0]);
        ProtocolException exception = assertThrows(ProtocolException.class, () -> FrameCodec.decode(bytes));
        assertEquals(ErrorCode.MALFORMED_FRAME, exception.errorCode());
    }

    @Test
    void rejectsOversizedPayloadLengthBeforeAllocation() throws Exception {
        byte[] bytes = rawFrame(ProtocolConstants.MAGIC, ProtocolConstants.VERSION,
                MessageType.SUCCESS.code(), 5,
                ProtocolConstants.MAX_PAYLOAD_BYTES + 1, new byte[0]);
        ProtocolException exception = assertThrows(ProtocolException.class, () -> FrameCodec.decode(bytes));
        assertEquals(ErrorCode.FRAME_TOO_LARGE, exception.errorCode());
    }

    @Test
    void rejectsTypeSpecificOversizeBeforeReadingPayload() throws Exception {
        int declared = ProtocolConstants.FILE_CHUNK_PREFIX_BYTES
                + ProtocolConstants.MAX_CHUNK_BYTES + 1;
        byte[] bytes = rawFrame(ProtocolConstants.MAGIC, ProtocolConstants.VERSION,
                MessageType.FILE_CHUNK.code(), 5, declared, new byte[0]);
        ProtocolException exception = assertThrows(ProtocolException.class, () -> FrameCodec.decode(bytes));
        assertEquals(ErrorCode.MALFORMED_FRAME, exception.errorCode());
    }

    @Test
    void decodeRejectsBytesAfterFirstFrame() throws Exception {
        byte[] valid = FrameCodec.encode(frame(MessageType.HELLO, new byte[0]));
        byte[] withTrailing = java.util.Arrays.copyOf(valid, valid.length + 1);
        assertThrows(ProtocolException.class, () -> FrameCodec.decode(withTrailing));
    }

    private static Frame frame(MessageType type, byte[] payload) {
        return new Frame(type, type.code(), payload);
    }

    private static void assertFrameEquals(Frame expected, Frame actual) {
        assertEquals(expected.type(), actual.type());
        assertEquals(expected.correlationId(), actual.correlationId());
        assertArrayEquals(expected.payload(), actual.payload());
    }

    private static byte[] rawFrame(
            int magic, int version, int type, long id, int payloadLength, byte[] payload)
            throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream output = new DataOutputStream(bytes);
        output.writeInt(magic);
        output.writeByte(version);
        output.writeByte(type);
        output.writeLong(id);
        output.writeInt(payloadLength);
        output.write(payload);
        return bytes.toByteArray();
    }

    private static final class OneByteAtATimeInputStream extends FilterInputStream {
        private OneByteAtATimeInputStream(InputStream input) {
            super(input);
        }

        @Override
        public int read(byte[] bytes, int offset, int length) throws IOException {
            return super.read(bytes, offset, Math.min(length, 1));
        }
    }
}
