package filewire.protocol;

import java.util.Arrays;

/** Message discriminator stored in every FileWire frame header. */
public enum MessageType {
    HELLO(1),
    LIST_REQUEST(2),
    LIST_RESPONSE(3),
    UPLOAD_REQUEST(4),
    UPLOAD_ACCEPTED(5),
    DOWNLOAD_REQUEST(6),
    DOWNLOAD_METADATA(7),
    FILE_CHUNK(8),
    TRANSFER_COMPLETE(9),
    DELETE_REQUEST(10),
    SUCCESS(11),
    ERROR(12),
    DISCONNECT(13),
    KEEPALIVE(14);

    private final int code;

    MessageType(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    public static MessageType fromCode(int code) throws ProtocolException {
        return Arrays.stream(values())
                .filter(type -> type.code == code)
                .findFirst()
                .orElseThrow(() -> new ProtocolException(
                        ErrorCode.UNSUPPORTED_MESSAGE,
                        "Unsupported message type: " + code));
    }
}
