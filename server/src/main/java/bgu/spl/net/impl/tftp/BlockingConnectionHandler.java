package bgu.spl.net.impl.tftp;

import bgu.spl.net.api.MessageEncoderDecoder;
import bgu.spl.net.api.MessagingProtocol;
import bgu.spl.net.srv.ConnectionHandler;
import bgu.spl.net.srv.Connections;
import bgu.spl.net.srv.ConnectionsImpl;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.LinkedList;

public class BlockingConnectionHandler<T> implements Runnable, ConnectionHandler<T> {

    private final TftpProtocol protocol;
    private final TftpEncoderDecoder encdec;
    private final Socket sock;
    private final ConnectionsImpl<byte[]> connctor;
    private BufferedInputStream in;
    private BufferedOutputStream out;
    private volatile boolean connected = true;

    public BlockingConnectionHandler(Socket sock, TftpEncoderDecoder reader, TftpProtocol protocol,ConnectionsImpl<byte[]>  connctor) {
        this.sock = sock;
        this.encdec = reader;
        this.protocol = protocol;
        this.connctor=connctor;
    }

    @Override
    public void run() {
        try (Socket sock = this.sock) { //just for automatic closing
            int read;
           
            protocol.start(connctor.getid(), connctor);
            protocol.setHandler(this);

            in = new BufferedInputStream(sock.getInputStream());
            out = new BufferedOutputStream(sock.getOutputStream());
            

            while (!protocol.shouldTerminate() && connected && (read=in.read()) != -1) {
              
                byte[] nextMessage = encdec.decodeNextByte((byte) read);
                    if (nextMessage != null   ) {
                        
                         protocol.process(nextMessage);
                         
                    
                }
                
            }
           

        } catch (IOException ex) {
            ex.printStackTrace();
        }

    }

    @Override
    public void close() throws IOException {
        connected = false;
        sock.close();
    }
  

    @Override
    public void send(byte[] msg) {
        
        try {
            if (msg != null) {
            
                out.write((byte[])encdec.encode(msg));
                
                out.flush();
               
                
            }
        } catch (IOException ex) {
            ex.printStackTrace();
}

    }
    
  
}