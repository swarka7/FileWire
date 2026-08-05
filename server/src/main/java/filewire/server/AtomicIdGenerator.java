package filewire.server;

import java.util.concurrent.atomic.AtomicLong;

/** Monotonic positive identifiers; exhaustion fails instead of wrapping and reusing an ID. */
final class AtomicIdGenerator {
    private final AtomicLong next;

    AtomicIdGenerator() {
        this(1);
    }

    AtomicIdGenerator(long firstId) {
        if (firstId <= 0) {
            throw new IllegalArgumentException("firstId must be positive");
        }
        next = new AtomicLong(firstId);
    }

    long nextId() {
        long id = next.getAndIncrement();
        if (id <= 0) {
            throw new IllegalStateException("identifier space exhausted");
        }
        return id;
    }
}
