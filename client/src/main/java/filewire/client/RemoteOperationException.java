package filewire.client;

import filewire.protocol.ErrorCode;

import java.io.IOException;
import java.util.Objects;

/** A structured error returned by the remote FileWire server. */
public final class RemoteOperationException extends IOException {
    private final ErrorCode errorCode;
    private final long correlationId;

    public RemoteOperationException(ErrorCode errorCode, long correlationId, String message) {
        super(message);
        this.errorCode = Objects.requireNonNull(errorCode, "errorCode");
        this.correlationId = correlationId;
    }

    public ErrorCode errorCode() {
        return errorCode;
    }

    public long correlationId() {
        return correlationId;
    }
}
