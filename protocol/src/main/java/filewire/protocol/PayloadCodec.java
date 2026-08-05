package filewire.protocol;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Encodes and strictly validates the type-specific payload inside a frame. */
public final class PayloadCodec {
    private PayloadCodec() {
    }

    public static byte[] encodeListResponse(ListResponse response) throws ProtocolException {
        Objects.requireNonNull(response, "response");
        if (response.filenames().size() > ProtocolConstants.MAX_LIST_ENTRIES) {
            throw malformed("Directory listing contains too many entries");
        }
        return encode(out -> {
            out.writeInt(response.filenames().size());
            for (String filename : response.filenames()) {
                writeFilename(out, filename);
            }
        });
    }

    public static ListResponse decodeListResponse(byte[] payload) throws ProtocolException {
        Reader reader = reader(payload);
        int count = reader.readInt("list entry count");
        if (count < 0 || count > ProtocolConstants.MAX_LIST_ENTRIES) {
            throw malformed("Invalid directory-list entry count: " + count);
        }
        List<String> filenames = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            filenames.add(reader.readFilename());
        }
        reader.requireFinished();
        return new ListResponse(filenames);
    }

    public static byte[] encodeUploadRequest(UploadRequest request) throws ProtocolException {
        Objects.requireNonNull(request, "request");
        return encode(out -> {
            writeFilename(out, request.filename());
            out.writeLong(requireNonNegative(request.size(), "file size"));
            out.write(request.internalDigest());
        });
    }

    public static UploadRequest decodeUploadRequest(byte[] payload) throws ProtocolException {
        Reader reader = reader(payload);
        String filename = reader.readFilename();
        long size = requireNonNegative(reader.readLong("file size"), "file size");
        byte[] digest = reader.readBytes(ProtocolConstants.SHA256_BYTES, "SHA-256 digest");
        reader.requireFinished();
        return new UploadRequest(filename, size, digest);
    }

    public static byte[] encodeUploadAccepted(UploadAccepted accepted) throws ProtocolException {
        Objects.requireNonNull(accepted, "accepted");
        return encode(out -> {
            out.writeLong(requirePositive(accepted.transferId(), "transfer ID"));
            out.writeInt(requireChunkSize(accepted.chunkSize()));
        });
    }

    public static UploadAccepted decodeUploadAccepted(byte[] payload) throws ProtocolException {
        Reader reader = reader(payload);
        long transferId = requirePositive(reader.readLong("transfer ID"), "transfer ID");
        int chunkSize = requireChunkSize(reader.readInt("chunk size"));
        reader.requireFinished();
        return new UploadAccepted(transferId, chunkSize);
    }

    public static byte[] encodeDownloadRequest(DownloadRequest request) throws ProtocolException {
        Objects.requireNonNull(request, "request");
        return encode(out -> writeFilename(out, request.filename()));
    }

    public static DownloadRequest decodeDownloadRequest(byte[] payload) throws ProtocolException {
        Reader reader = reader(payload);
        String filename = reader.readFilename();
        reader.requireFinished();
        return new DownloadRequest(filename);
    }

    public static byte[] encodeDownloadMetadata(DownloadMetadata metadata) throws ProtocolException {
        Objects.requireNonNull(metadata, "metadata");
        return encode(out -> {
            out.writeLong(requirePositive(metadata.transferId(), "transfer ID"));
            writeFilename(out, metadata.filename());
            out.writeLong(requireNonNegative(metadata.size(), "file size"));
            out.write(metadata.internalDigest());
            out.writeInt(requireChunkSize(metadata.chunkSize()));
        });
    }

    public static DownloadMetadata decodeDownloadMetadata(byte[] payload) throws ProtocolException {
        Reader reader = reader(payload);
        long transferId = requirePositive(reader.readLong("transfer ID"), "transfer ID");
        String filename = reader.readFilename();
        long size = requireNonNegative(reader.readLong("file size"), "file size");
        byte[] digest = reader.readBytes(ProtocolConstants.SHA256_BYTES, "SHA-256 digest");
        int chunkSize = requireChunkSize(reader.readInt("chunk size"));
        reader.requireFinished();
        return new DownloadMetadata(transferId, filename, size, digest, chunkSize);
    }

    public static byte[] encodeFileChunk(FileChunk chunk) throws ProtocolException {
        Objects.requireNonNull(chunk, "chunk");
        validateChunk(chunk.sequence(), chunk.finalChunk(), chunk.internalData().length);
        return encode(out -> {
            out.writeInt(chunk.sequence());
            out.writeByte(chunk.finalChunk() ? 1 : 0);
            out.writeInt(chunk.internalData().length);
            out.write(chunk.internalData());
        });
    }

    public static FileChunk decodeFileChunk(byte[] payload) throws ProtocolException {
        Reader reader = reader(payload);
        int sequence = reader.readInt("chunk sequence");
        int finalFlag = reader.readUnsignedByte("final-chunk flag");
        if (finalFlag != 0 && finalFlag != 1) {
            throw malformed("Final-chunk flag must be 0 or 1");
        }
        int chunkLength = reader.readInt("chunk length");
        validateChunk(sequence, finalFlag == 1, chunkLength);
        if (chunkLength != reader.remaining()) {
            throw malformed("Declared chunk length does not match the payload");
        }
        byte[] data = reader.readBytes(chunkLength, "chunk bytes");
        reader.requireFinished();
        return new FileChunk(sequence, finalFlag == 1, data);
    }

    public static byte[] encodeTransferComplete(TransferComplete complete) throws ProtocolException {
        Objects.requireNonNull(complete, "complete");
        return encode(out -> {
            out.writeLong(requireNonNegative(complete.totalBytes(), "transferred byte count"));
            out.write(complete.internalDigest());
        });
    }

    public static TransferComplete decodeTransferComplete(byte[] payload) throws ProtocolException {
        Reader reader = reader(payload);
        long totalBytes = requireNonNegative(
                reader.readLong("transferred byte count"), "transferred byte count");
        byte[] digest = reader.readBytes(ProtocolConstants.SHA256_BYTES, "SHA-256 digest");
        reader.requireFinished();
        return new TransferComplete(totalBytes, digest);
    }

    public static byte[] encodeDeleteRequest(DeleteRequest request) throws ProtocolException {
        Objects.requireNonNull(request, "request");
        return encode(out -> writeFilename(out, request.filename()));
    }

    public static DeleteRequest decodeDeleteRequest(byte[] payload) throws ProtocolException {
        Reader reader = reader(payload);
        String filename = reader.readFilename();
        reader.requireFinished();
        return new DeleteRequest(filename);
    }

    public static byte[] encodeSuccess(Success success) throws ProtocolException {
        Objects.requireNonNull(success, "success");
        return encode(out -> writeString(
                out, success.message(), ProtocolConstants.MAX_STATUS_MESSAGE_BYTES, "status message"));
    }

    public static Success decodeSuccess(byte[] payload) throws ProtocolException {
        Reader reader = reader(payload);
        String message = reader.readString(
                ProtocolConstants.MAX_STATUS_MESSAGE_BYTES, "status message");
        reader.requireFinished();
        return new Success(message);
    }

    public static byte[] encodeError(Error error) throws ProtocolException {
        Objects.requireNonNull(error, "error");
        return encode(out -> {
            out.writeShort(error.code().code());
            writeString(out, error.message(), ProtocolConstants.MAX_STATUS_MESSAGE_BYTES, "error message");
        });
    }

    public static Error decodeError(byte[] payload) throws ProtocolException {
        Reader reader = reader(payload);
        ErrorCode code = ErrorCode.fromCode(reader.readUnsignedShort("error code"));
        String message = reader.readString(
                ProtocolConstants.MAX_STATUS_MESSAGE_BYTES, "error message");
        reader.requireFinished();
        return new Error(code, message);
    }

    /** Fully parses a payload so malformed inner lengths and UTF-8 never reach a session. */
    public static void validate(MessageType type, byte[] payload) throws ProtocolException {
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(payload, "payload");
        validateDeclaredLength(type, payload.length);
        switch (type) {
            case HELLO, LIST_REQUEST, DISCONNECT, KEEPALIVE -> {
                // The exact zero length was checked above.
            }
            case LIST_RESPONSE -> decodeListResponse(payload);
            case UPLOAD_REQUEST -> decodeUploadRequest(payload);
            case UPLOAD_ACCEPTED -> decodeUploadAccepted(payload);
            case DOWNLOAD_REQUEST -> decodeDownloadRequest(payload);
            case DOWNLOAD_METADATA -> decodeDownloadMetadata(payload);
            case FILE_CHUNK -> decodeFileChunk(payload);
            case TRANSFER_COMPLETE -> decodeTransferComplete(payload);
            case DELETE_REQUEST -> decodeDeleteRequest(payload);
            case SUCCESS -> decodeSuccess(payload);
            case ERROR -> decodeError(payload);
        }
    }

    static void validateDeclaredLength(MessageType type, int payloadLength) throws ProtocolException {
        if (payloadLength < 0) {
            throw malformed("Negative payload length");
        }
        if (payloadLength > ProtocolConstants.MAX_PAYLOAD_BYTES) {
            throw new ProtocolException(ErrorCode.FRAME_TOO_LARGE, "Frame payload exceeds the protocol limit");
        }

        switch (type) {
            case HELLO, LIST_REQUEST, DISCONNECT, KEEPALIVE ->
                    requireLength(type, payloadLength, 0, 0);
            case LIST_RESPONSE -> requireLength(
                    type, payloadLength, Integer.BYTES, ProtocolConstants.MAX_PAYLOAD_BYTES);
            case UPLOAD_REQUEST -> requireLength(
                    type,
                    payloadLength,
                    Short.BYTES + 1 + Long.BYTES + ProtocolConstants.SHA256_BYTES,
                    Short.BYTES + ProtocolConstants.MAX_FILENAME_BYTES
                            + Long.BYTES + ProtocolConstants.SHA256_BYTES);
            case UPLOAD_ACCEPTED -> requireLength(
                    type, payloadLength, Long.BYTES + Integer.BYTES, Long.BYTES + Integer.BYTES);
            case DOWNLOAD_REQUEST, DELETE_REQUEST -> requireLength(
                    type,
                    payloadLength,
                    Short.BYTES + 1,
                    Short.BYTES + ProtocolConstants.MAX_FILENAME_BYTES);
            case DOWNLOAD_METADATA -> requireLength(
                    type,
                    payloadLength,
                    Long.BYTES + Short.BYTES + 1 + Long.BYTES
                            + ProtocolConstants.SHA256_BYTES + Integer.BYTES,
                    Long.BYTES + Short.BYTES + ProtocolConstants.MAX_FILENAME_BYTES + Long.BYTES
                            + ProtocolConstants.SHA256_BYTES + Integer.BYTES);
            case FILE_CHUNK -> requireLength(
                    type,
                    payloadLength,
                    ProtocolConstants.FILE_CHUNK_PREFIX_BYTES,
                    ProtocolConstants.FILE_CHUNK_PREFIX_BYTES + ProtocolConstants.MAX_CHUNK_BYTES);
            case TRANSFER_COMPLETE -> requireLength(
                    type,
                    payloadLength,
                    Long.BYTES + ProtocolConstants.SHA256_BYTES,
                    Long.BYTES + ProtocolConstants.SHA256_BYTES);
            case SUCCESS -> requireLength(
                    type,
                    payloadLength,
                    Short.BYTES,
                    Short.BYTES + ProtocolConstants.MAX_STATUS_MESSAGE_BYTES);
            case ERROR -> requireLength(
                    type,
                    payloadLength,
                    Short.BYTES + Short.BYTES,
                    Short.BYTES + Short.BYTES + ProtocolConstants.MAX_STATUS_MESSAGE_BYTES);
        }
    }

    private static void requireLength(MessageType type, int actual, int minimum, int maximum)
            throws ProtocolException {
        if (actual < minimum || actual > maximum) {
            throw malformed("Invalid payload length " + actual + " for " + type);
        }
    }

    private static void validateChunk(int sequence, boolean finalChunk, int length)
            throws ProtocolException {
        if (sequence < 0) {
            throw malformed("Chunk sequence must not be negative");
        }
        if (length < 0 || length > ProtocolConstants.MAX_CHUNK_BYTES) {
            throw malformed("Chunk length is outside the protocol limit");
        }
        if (length == 0 && !finalChunk) {
            throw malformed("An empty chunk must be final");
        }
    }

    private static long requirePositive(long value, String field) throws ProtocolException {
        if (value <= 0) {
            throw malformed(field + " must be positive");
        }
        return value;
    }

    private static long requireNonNegative(long value, String field) throws ProtocolException {
        if (value < 0) {
            throw malformed(field + " must not be negative");
        }
        return value;
    }

    private static int requireChunkSize(int chunkSize) throws ProtocolException {
        if (chunkSize <= 0 || chunkSize > ProtocolConstants.MAX_CHUNK_BYTES) {
            throw malformed("Chunk size is outside the protocol limit");
        }
        return chunkSize;
    }

    private static byte[] encode(Encoder encoder) throws ProtocolException {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                encoder.write(output);
                output.flush();
            }
            byte[] payload = bytes.toByteArray();
            if (payload.length > ProtocolConstants.MAX_PAYLOAD_BYTES) {
                throw new ProtocolException(
                        ErrorCode.FRAME_TOO_LARGE, "Encoded payload exceeds the protocol limit");
            }
            return payload;
        } catch (ProtocolException exception) {
            throw exception;
        } catch (IOException impossible) {
            throw new ProtocolException(
                    ErrorCode.INTERNAL_ERROR,
                    ProtocolException.UNKNOWN_CORRELATION_ID,
                    false,
                    "Could not encode an in-memory payload",
                    impossible);
        }
    }

    private static void writeFilename(DataOutputStream output, String filename)
            throws IOException, ProtocolException {
        FilenameValidator.requireValid(filename);
        writeString(output, filename, ProtocolConstants.MAX_FILENAME_BYTES, "filename");
    }

    private static void writeString(
            DataOutputStream output, String value, int maximumBytes, String field)
            throws IOException, ProtocolException {
        Objects.requireNonNull(value, field);
        byte[] encoded = strictUtf8(value, field);
        if (encoded.length > maximumBytes) {
            throw malformed(field + " exceeds " + maximumBytes + " UTF-8 bytes");
        }
        output.writeShort(encoded.length);
        output.write(encoded);
    }

    private static byte[] strictUtf8(String value, String field) throws ProtocolException {
        try {
            ByteBuffer encoded = StandardCharsets.UTF_8.newEncoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .encode(CharBuffer.wrap(value));
            byte[] result = new byte[encoded.remaining()];
            encoded.get(result);
            return result;
        } catch (CharacterCodingException exception) {
            throw new ProtocolException(
                    ErrorCode.MALFORMED_FRAME,
                    ProtocolException.UNKNOWN_CORRELATION_ID,
                    false,
                    field + " is not valid Unicode",
                    exception);
        }
    }

    private static Reader reader(byte[] payload) {
        return new Reader(Objects.requireNonNull(payload, "payload"));
    }

    private static ProtocolException malformed(String message) {
        return new ProtocolException(ErrorCode.MALFORMED_FRAME, message);
    }

    @FunctionalInterface
    private interface Encoder {
        void write(DataOutputStream output) throws IOException;
    }

    private static final class Reader {
        private final ByteBuffer input;

        private Reader(byte[] payload) {
            this.input = ByteBuffer.wrap(payload);
        }

        private int remaining() {
            return input.remaining();
        }

        private int readUnsignedByte(String field) throws ProtocolException {
            requireRemaining(Byte.BYTES, field);
            return Byte.toUnsignedInt(input.get());
        }

        private int readUnsignedShort(String field) throws ProtocolException {
            requireRemaining(Short.BYTES, field);
            return Short.toUnsignedInt(input.getShort());
        }

        private int readInt(String field) throws ProtocolException {
            requireRemaining(Integer.BYTES, field);
            return input.getInt();
        }

        private long readLong(String field) throws ProtocolException {
            requireRemaining(Long.BYTES, field);
            return input.getLong();
        }

        private byte[] readBytes(int length, String field) throws ProtocolException {
            if (length < 0) {
                throw malformed("Negative " + field + " length");
            }
            requireRemaining(length, field);
            byte[] bytes = new byte[length];
            input.get(bytes);
            return bytes;
        }

        private String readFilename() throws ProtocolException {
            String filename = readString(ProtocolConstants.MAX_FILENAME_BYTES, "filename");
            return FilenameValidator.requireValid(filename);
        }

        private String readString(int maximumBytes, String field) throws ProtocolException {
            int length = readUnsignedShort(field + " length");
            if (length > maximumBytes) {
                throw malformed(field + " exceeds " + maximumBytes + " UTF-8 bytes");
            }
            byte[] encoded = readBytes(length, field);
            try {
                return StandardCharsets.UTF_8.newDecoder()
                        .onMalformedInput(CodingErrorAction.REPORT)
                        .onUnmappableCharacter(CodingErrorAction.REPORT)
                        .decode(ByteBuffer.wrap(encoded))
                        .toString();
            } catch (CharacterCodingException exception) {
                throw new ProtocolException(
                        ErrorCode.MALFORMED_FRAME,
                        ProtocolException.UNKNOWN_CORRELATION_ID,
                        false,
                        field + " is not valid UTF-8",
                        exception);
            }
        }

        private void requireRemaining(int required, String field) throws ProtocolException {
            if (required < 0 || input.remaining() < required) {
                throw malformed("Truncated " + field);
            }
        }

        private void requireFinished() throws ProtocolException {
            if (input.hasRemaining()) {
                throw malformed("Payload contains trailing bytes");
            }
        }
    }

    public record ListResponse(List<String> filenames) {
        public ListResponse {
            filenames = List.copyOf(Objects.requireNonNull(filenames, "filenames"));
        }
    }

    public record UploadRequest(String filename, long size, byte[] sha256) {
        public UploadRequest {
            Objects.requireNonNull(filename, "filename");
            if (size < 0) {
                throw new IllegalArgumentException("size must not be negative");
            }
            sha256 = copyDigest(sha256);
        }

        @Override
        public byte[] sha256() {
            return sha256.clone();
        }

        private byte[] internalDigest() {
            return sha256;
        }
    }

    public record UploadAccepted(long transferId, int chunkSize) {
        public UploadAccepted {
            if (transferId <= 0) {
                throw new IllegalArgumentException("transferId must be positive");
            }
            if (chunkSize <= 0 || chunkSize > ProtocolConstants.MAX_CHUNK_BYTES) {
                throw new IllegalArgumentException("chunkSize is outside the protocol limit");
            }
        }
    }

    public record DownloadRequest(String filename) {
        public DownloadRequest {
            Objects.requireNonNull(filename, "filename");
        }
    }

    public record DownloadMetadata(
            long transferId,
            String filename,
            long size,
            byte[] sha256,
            int chunkSize) {
        public DownloadMetadata {
            if (transferId <= 0) {
                throw new IllegalArgumentException("transferId must be positive");
            }
            Objects.requireNonNull(filename, "filename");
            if (size < 0) {
                throw new IllegalArgumentException("size must not be negative");
            }
            sha256 = copyDigest(sha256);
            if (chunkSize <= 0 || chunkSize > ProtocolConstants.MAX_CHUNK_BYTES) {
                throw new IllegalArgumentException("chunkSize is outside the protocol limit");
            }
        }

        @Override
        public byte[] sha256() {
            return sha256.clone();
        }

        private byte[] internalDigest() {
            return sha256;
        }
    }

    public record FileChunk(int sequence, boolean finalChunk, byte[] data) {
        public FileChunk {
            if (sequence < 0) {
                throw new IllegalArgumentException("sequence must not be negative");
            }
            Objects.requireNonNull(data, "data");
            if (data.length > ProtocolConstants.MAX_CHUNK_BYTES) {
                throw new IllegalArgumentException("data exceeds the chunk limit");
            }
            if (data.length == 0 && !finalChunk) {
                throw new IllegalArgumentException("an empty chunk must be final");
            }
            data = data.clone();
        }

        @Override
        public byte[] data() {
            return data.clone();
        }

        private byte[] internalData() {
            return data;
        }
    }

    public record TransferComplete(long totalBytes, byte[] sha256) {
        public TransferComplete {
            if (totalBytes < 0) {
                throw new IllegalArgumentException("totalBytes must not be negative");
            }
            sha256 = copyDigest(sha256);
        }

        @Override
        public byte[] sha256() {
            return sha256.clone();
        }

        private byte[] internalDigest() {
            return sha256;
        }
    }

    public record DeleteRequest(String filename) {
        public DeleteRequest {
            Objects.requireNonNull(filename, "filename");
        }
    }

    public record Success(String message) {
        public Success {
            Objects.requireNonNull(message, "message");
        }
    }

    public record Error(ErrorCode code, String message) {
        public Error {
            Objects.requireNonNull(code, "code");
            Objects.requireNonNull(message, "message");
        }
    }

    private static byte[] copyDigest(byte[] digest) {
        Objects.requireNonNull(digest, "sha256");
        if (digest.length != ProtocolConstants.SHA256_BYTES) {
            throw new IllegalArgumentException(
                    "SHA-256 digest must contain exactly " + ProtocolConstants.SHA256_BYTES + " bytes");
        }
        return digest.clone();
    }
}
