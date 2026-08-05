package filewire.protocol;

/** Wire-level constants shared by FileWire clients and servers. */
public final class ProtocolConstants {
    public static final int MAGIC = 0x4657_4952; // "FWIR"
    public static final int VERSION = 1;
    public static final int HEADER_BYTES = Integer.BYTES + Byte.BYTES + Byte.BYTES
            + Long.BYTES + Integer.BYTES;

    public static final int MAX_PAYLOAD_BYTES = 1024 * 1024;
    public static final int MAX_FRAME_BYTES = HEADER_BYTES + MAX_PAYLOAD_BYTES;
    public static final int MAX_CHUNK_BYTES = 64 * 1024;
    public static final int MAX_FILENAME_BYTES = 255;
    public static final int MAX_STATUS_MESSAGE_BYTES = 1024;
    public static final int SHA256_BYTES = 32;
    public static final int MAX_LIST_ENTRIES = 4096;
    public static final int PREPARATION_KEEPALIVE_INTERVAL_MILLIS = 1_000;

    public static final int FILE_CHUNK_PREFIX_BYTES = Integer.BYTES + Byte.BYTES + Integer.BYTES;

    private ProtocolConstants() {
    }
}
