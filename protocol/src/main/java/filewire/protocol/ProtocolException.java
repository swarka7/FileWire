package filewire.protocol;

import java.io.IOException;

/** Signals a non-recoverable violation of the FileWire wire format. */
public final class ProtocolException extends IOException {
    public static final long UNKNOWN_CORRELATION_ID = 0L;

    private final ErrorCode errorCode;
    private final long correlationId;
    private final boolean replySafe;

    public ProtocolException(ErrorCode errorCode, String message) {
        this(errorCode, UNKNOWN_CORRELATION_ID, false, message, null);
    }

    public ProtocolException(
            ErrorCode errorCode,
            long correlationId,
            boolean replySafe,
            String message) {
        this(errorCode, correlationId, replySafe, message, null);
    }

    public ProtocolException(
            ErrorCode errorCode,
            long correlationId,
            boolean replySafe,
            String message,
            Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
        this.correlationId = correlationId;
        this.replySafe = replySafe;
    }

    public ErrorCode errorCode() {
        return errorCode;
    }

    public long correlationId() {
        return correlationId;
    }

    public boolean replySafe() {
        return replySafe;
    }

    /** Frame violations are terminal because the stream cannot be safely resynchronized. */
    public boolean recoverable() {
        return false;
    }
}
