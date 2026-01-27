package bgu.spl.net.impl.tftp;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

import bgu.spl.net.api.MessageEncoderDecoder;
import bgu.spl.net.api.MessagingProtocol;

public class TftpClient {

    public static void main(String[] args) throws IOException {
        Scanner t = new Scanner(System.in);
        ClientEncoderDecoder encdec = new ClientEncoderDecoder();
        TftpProtocol protocol = new TftpProtocol();
        boolean connected = false;

        if (args.length < 2) {
            System.out.println("you must supply two arguments: host, message");
            System.exit(1);
        }

        String host = args[0];
        int port = Integer.parseInt(args[1]);
        Socket sock = new Socket(host, port);
        BufferedInputStream in = new BufferedInputStream(sock.getInputStream());
        BufferedOutputStream out = new BufferedOutputStream(sock.getOutputStream());

        Thread listeningThread = new Thread(() -> {
            try {
                int read;
                while(!Thread.currentThread().isInterrupted() && (read = in.read()) >=0 ){
                    byte[] nextMessage = encdec.decodeNextByte((byte)read);
                    if(nextMessage != null){
                        byte[] tmp = protocol.process(nextMessage);
                        if(tmp != null){
                            out.write(encdec.encode(tmp));
                            out.flush();
                        }
                    }
                }
            
            }catch (IOException e) {
                e.printStackTrace();
            }
            System.out.println("Listening thread terminated.");
        });
        listeningThread.start();

        Thread keyboardThread = new Thread(() -> {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    String command = t.nextLine();
                    if (command.trim().isEmpty()) {
                        continue;
                    }
                    String[] words = command.split(" ",2);
                    String op = words[0];
                    String arg = words.length > 1 ? words[1].trim() : "";

                    if (op.equals("LOGRQ") && !arg.isEmpty()) {
                        byte[] nameBytes = arg.getBytes(StandardCharsets.UTF_8);
                        byte[] tmp = new byte[nameBytes.length + 3];
                        tmp[0] = 0x0;
                        tmp[1] = 0x7;
                        for (int i = 0; i < nameBytes.length; i++) {
                            tmp[i + 2] = nameBytes[i];
                        }
                        tmp[tmp.length - 1] = 0x0;
                        out.write(tmp);
                        out.flush();
                    }

                    else if (op.equals("DELRQ") && !arg.isEmpty()) {
                        byte[] nameBytes = arg.getBytes(StandardCharsets.UTF_8);
                        byte[] tmp = new byte[nameBytes.length + 3];
                        tmp[0] = 0x0;
                        tmp[1] = 0x8;
                        for (int i = 0; i < nameBytes.length; i++) {
                            tmp[i + 2] = nameBytes[i];
                        }
                        tmp[tmp.length - 1] = 0x0;
                        out.write(tmp);
                        out.flush();
                    }

                    else if (op.equals("RRQ") && !arg.isEmpty()) {
                       protocol.CreateFile(arg);
                      
                        byte[] nameBytes = arg.getBytes(StandardCharsets.UTF_8);
                        byte[] tmp = new byte[nameBytes.length + 3];
                        tmp[0] = 0x0;
                        tmp[1] = 0x1;
                        for (int i = 0; i < nameBytes.length; i++) {
                            tmp[i + 2] = nameBytes[i];
                        }
                        tmp[tmp.length - 1] = 0x0;
                        out.write(tmp);
                        out.flush();
                    }

                    else if (op.equals("WRQ") && !arg.isEmpty()) {
                        byte[] nameBytes = arg.getBytes(StandardCharsets.UTF_8);
                        boolean ready = protocol.WRQ(nameBytes);
                        if(!ready){
                            continue;
                        }
                        protocol.iswrq=true;
                        byte[] tmp = new byte[nameBytes.length + 3];
                      
                        tmp[0] = 0x0;
                        tmp[1] = 0x2;
                        for (int i = 0; i < nameBytes.length; i++) {
                            tmp[i + 2] = nameBytes[i];
                        }
                        tmp[tmp.length - 1] = 0x0;
                        out.write(tmp);
                        out.flush();
                    }

                    else if (op.equals("DIRQ")) {
                        protocol.startDirq();
                        byte[] tmp = new byte[2];
                        tmp[0] = 0x0;
                        tmp[1] = 0x6;
                        out.write(tmp);
                        out.flush();
                    }

                    else if (op.equals("DISC")) {
                        byte[] tmp = new byte[2];
                        tmp[0] = 0x0;
                        tmp[1] = 0xA;
                        out.write(tmp);
                        out.flush();
                        if ( protocol.isConnected()) {
                            try {
                                listeningThread.join();
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                            listeningThread.interrupt();
                            Thread.currentThread().interrupt();
                        }

                    }

                    else {
                        System.out.println("Invalid command.");
                    }

                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            System.out.println("KeyBoard thread terminated.");
            
        });
        keyboardThread.start();
        
    }

}
