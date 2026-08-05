package filewire.protocol;

import java.util.Arrays;

/** Structured errors that may be carried by an {@link MessageType#ERROR} frame. */
public enum ErrorCode {
    INVALID_REQUEST(1),
    INVALID_FILENAME(2),
    FILE_NOT_FOUND(3),
    FILE_ALREADY_EXISTS(4),
    TRANSFER_CONFLICT(5),
    INTEGRITY_MISMATCH(6),
    FRAME_TOO_LARGE(7),
    UNSUPPORTED_MESSAGE(8),
    MALFORMED_FRAME(9),
    INTERNAL_ERROR(10),
    SERVER_BUSY(11),
    UNSUPPORTED_VERSION(12),
    TRANSFER_NOT_FOUND(13),
    IO_FAILURE(14);

    private final int code;

    ErrorCode(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    public static ErrorCode fromCode(int code) throws ProtocolException {
        return Arrays.stream(values())
                .filter(error -> error.code == code)
                .findFirst()
                .orElseThrow(() -> new ProtocolException(
                        MALFORMED_FRAME,
                        "Unknown error code: " + code));
    }
}
