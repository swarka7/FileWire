package filewire.protocol;

import java.util.Objects;

/** An immutable FileWire frame after its fixed header has been decoded. */
public record Frame(MessageType type, long correlationId, byte[] payload) {
    public Frame {
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(payload, "payload");
        if (correlationId <= 0) {
            throw new IllegalArgumentException("correlationId must be positive");
        }
        if (payload.length > ProtocolConstants.MAX_PAYLOAD_BYTES) {
            throw new IllegalArgumentException("payload exceeds the protocol limit");
        }
        payload = payload.clone();
    }

    @Override
    public byte[] payload() {
        return payload.clone();
    }

    byte[] internalPayload() {
        return payload;
    }
}
