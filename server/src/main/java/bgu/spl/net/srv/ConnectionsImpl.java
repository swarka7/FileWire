package bgu.spl.net.srv;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Handler;

public class ConnectionsImpl<T> implements Connections<T> {
    private Map<Integer, ConnectionHandler<T>> activeConnections = new ConcurrentHashMap<>();
    private Map<Integer, String> connectionNames = new ConcurrentHashMap<>(); // Map connection IDs to names
    private int id=0;

    @Override
    public void connect(int connectionId, ConnectionHandler<T> handler) {
        activeConnections.put(connectionId, handler);
        id++;
    }

    @Override
    public boolean send(int connectionId, T msg) {
       ConnectionHandler<T> handler = activeConnections.get(connectionId);
        
        if (handler != null) {
            handler.send((byte[]) msg);
            return true;
        }
        return false;
    }

    @Override
    public void disconnect(int connectionId) {
        activeConnections.remove(connectionId);
        // Remove the associated name
        connectionNames.remove(connectionId);
    }

    public void addName(int connectionId, String name) {
        connectionNames.put(connectionId, name);
    }

    public boolean containsName(String name) {
        return connectionNames.containsValue(name);
    }

    public List<Integer> getLoggedInClients() {
        return new ArrayList<>(connectionNames.keySet());
    }
    public List<Integer> getConnectedClients() {
        List<Integer> connectedClients = new ArrayList<>();
        for (Integer connectionId : activeConnections.keySet()) {
            connectedClients.add(connectionId);
        }
        return connectedClients;
    }
    public int getid(){
        return id;
    }
}
