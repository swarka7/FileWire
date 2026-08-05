package filewire.client;

import filewire.protocol.ErrorCode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RemoteOperationExceptionTest {
    @Test
    void preservesStructuredRemoteErrorDetails() {
        RemoteOperationException exception = new RemoteOperationException(
                ErrorCode.FILE_ALREADY_EXISTS, 41, "File already exists");

        assertEquals(ErrorCode.FILE_ALREADY_EXISTS, exception.errorCode());
        assertEquals(41, exception.correlationId());
        assertEquals("File already exists", exception.getMessage());
    }
}
