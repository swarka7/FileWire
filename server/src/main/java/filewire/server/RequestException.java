package filewire.server;

import filewire.protocol.ErrorCode;

import java.io.IOException;
import java.util.Objects;

/** An expected request failure that can be returned to a client without exposing internals. */
final class RequestException extends IOException {
    private final ErrorCode errorCode;

    RequestException(ErrorCode errorCode, String message) {
        super(message);
        this.errorCode = Objects.requireNonNull(errorCode, "errorCode");
    }

    RequestException(ErrorCode errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = Objects.requireNonNull(errorCode, "errorCode");
    }

    ErrorCode errorCode() {
        return errorCode;
    }
}
