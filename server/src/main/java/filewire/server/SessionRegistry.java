package filewire.server;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Thread-safe ownership of live sessions and their monotonic identifiers. */
final class SessionRegistry {
    private final AtomicIdGenerator ids = new AtomicIdGenerator();
    private final Map<Long, ClientSession> sessions = new ConcurrentHashMap<>();

    long nextSessionId() {
        return ids.nextId();
    }

    void register(long sessionId, ClientSession session) {
        if (sessions.putIfAbsent(sessionId, session) != null) {
            throw new IllegalStateException("Duplicate session ID: " + sessionId);
        }
    }

    void unregister(long sessionId, ClientSession session) {
        sessions.remove(sessionId, session);
    }

    int size() {
        return sessions.size();
    }

    Collection<ClientSession> snapshot() {
        return new ArrayList<>(sessions.values());
    }
}
