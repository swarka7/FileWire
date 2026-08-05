package filewire.protocol;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Objects;

/** Reads and writes deterministic, length-prefixed FileWire frames. */
public final class FrameCodec {
    private FrameCodec() {
    }

    /**
     * Reads one frame. A clean EOF before any header byte returns {@code null}; every partial or
     * malformed frame throws a non-recoverable {@link ProtocolException}.
     */
    public static Frame read(InputStream input) throws IOException {
        Objects.requireNonNull(input, "input");

        int first = input.read();
        if (first == -1) {
            return null;
        }

        byte[] headerBytes = new byte[ProtocolConstants.HEADER_BYTES];
        headerBytes[0] = (byte) first;
        try {
            readFully(input, headerBytes, 1, headerBytes.length - 1);
        } catch (EOFException truncated) {
            throw new ProtocolException(
                    ErrorCode.MALFORMED_FRAME,
                    ProtocolException.UNKNOWN_CORRELATION_ID,
                    false,
                    "Truncated frame header",
                    truncated);
        }

        ByteBuffer header = ByteBuffer.wrap(headerBytes);
        int magic = header.getInt();
        int version = Byte.toUnsignedInt(header.get());
        int typeCode = Byte.toUnsignedInt(header.get());
        long correlationId = header.getLong();
        int payloadLength = header.getInt();

        if (magic != ProtocolConstants.MAGIC) {
            throw new ProtocolException(ErrorCode.MALFORMED_FRAME, "Invalid FileWire magic value");
        }
        if (version != ProtocolConstants.VERSION) {
            throw new ProtocolException(
                    ErrorCode.UNSUPPORTED_VERSION,
                    correlationId,
                    false,
                    "Unsupported protocol version: " + version);
        }
        if (correlationId <= 0) {
            throw new ProtocolException(
                    ErrorCode.INVALID_REQUEST,
                    ProtocolException.UNKNOWN_CORRELATION_ID,
                    false,
                    "Correlation or transfer ID must be positive");
        }

        MessageType type;
        try {
            type = MessageType.fromCode(typeCode);
        } catch (ProtocolException exception) {
            throw contextualize(exception, correlationId, true);
        }

        try {
            PayloadCodec.validateDeclaredLength(type, payloadLength);
        } catch (ProtocolException exception) {
            throw contextualize(exception, correlationId, true);
        }

        byte[] payload = new byte[payloadLength];
        try {
            readFully(input, payload, 0, payload.length);
        } catch (EOFException truncated) {
            throw new ProtocolException(
                    ErrorCode.MALFORMED_FRAME,
                    correlationId,
                    false,
                    "Truncated frame payload",
                    truncated);
        }

        try {
            PayloadCodec.validate(type, payload);
        } catch (ProtocolException exception) {
            throw contextualize(exception, correlationId, true);
        }
        return new Frame(type, correlationId, payload);
    }

    public static Frame readFrame(InputStream input) throws IOException {
        return read(input);
    }

    /** Writes exactly one frame and leaves flushing policy to the connection owner. */
    public static void write(OutputStream output, Frame frame) throws IOException {
        Objects.requireNonNull(output, "output");
        Objects.requireNonNull(frame, "frame");
        byte[] payload = frame.internalPayload();
        try {
            PayloadCodec.validateDeclaredLength(frame.type(), payload.length);
            PayloadCodec.validate(frame.type(), payload);
        } catch (ProtocolException exception) {
            throw contextualize(exception, frame.correlationId(), false);
        }

        DataOutputStream data = new DataOutputStream(output);
        data.writeInt(ProtocolConstants.MAGIC);
        data.writeByte(ProtocolConstants.VERSION);
        data.writeByte(frame.type().code());
        data.writeLong(frame.correlationId());
        data.writeInt(payload.length);
        data.write(payload);
    }

    public static void writeFrame(OutputStream output, Frame frame) throws IOException {
        write(output, frame);
    }

    public static byte[] encode(Frame frame) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream(
                ProtocolConstants.HEADER_BYTES + frame.internalPayload().length);
        write(output, frame);
        return output.toByteArray();
    }

    /** Decodes exactly one frame and rejects trailing bytes. */
    public static Frame decode(byte[] encoded) throws IOException {
        Objects.requireNonNull(encoded, "encoded");
        ByteArrayInputStream input = new ByteArrayInputStream(encoded);
        Frame frame = read(input);
        if (frame == null) {
            throw new ProtocolException(ErrorCode.MALFORMED_FRAME, "No frame was provided");
        }
        if (input.available() != 0) {
            throw new ProtocolException(
                    ErrorCode.MALFORMED_FRAME,
                    frame.correlationId(),
                    true,
                    "Encoded input contains bytes after the first frame");
        }
        return frame;
    }

    private static ProtocolException contextualize(
            ProtocolException exception, long correlationId, boolean replySafe) {
        return new ProtocolException(
                exception.errorCode(),
                correlationId,
                replySafe,
                exception.getMessage(),
                exception);
    }

    private static void readFully(InputStream input, byte[] target, int offset, int length)
            throws IOException {
        int cursor = offset;
        int end = offset + length;
        while (cursor < end) {
            int read = input.read(target, cursor, end - cursor);
            if (read == -1) {
                throw new EOFException();
            }
            if (read == 0) {
                int single = input.read();
                if (single == -1) {
                    throw new EOFException();
                }
                target[cursor++] = (byte) single;
            } else {
                cursor += read;
            }
        }
    }
}
