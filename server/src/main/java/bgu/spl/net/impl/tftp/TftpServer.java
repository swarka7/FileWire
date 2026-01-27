package bgu.spl.net.impl.tftp;

import bgu.spl.net.srv.ConnectionsImpl;

public class TftpServer {
    public static void main(String[] args) {
        // you can use any server... 
        ConnectionsImpl<byte[]> connections = new ConnectionsImpl<>();
        int port = 7777;
        if (args != null && args.length > 0) {
            try {
                port = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                System.out.println("Invalid port, using default 7777");
            }
        }
        Server.threadPerClient(
                port, //port
                () -> new TftpProtocol(),
                TftpEncoderDecoder::new ,connections,0
        ).serve();
        

     }

}
