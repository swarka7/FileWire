package bgu.spl.net.srv;

import java.io.IOException;
import java.util.List;

public interface Connections<T> {

    void connect(int connectionId, ConnectionHandler<T> handler);

    boolean send(int connectionId, T msg);

    void disconnect(int connectionId);

    void addName(int connectionId,String name);
    
    boolean containsName(String name);
    
    List<Integer> getConnectedClients();
}
