package cn.edu.ruc.iir.pard.client;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.util.Scanner;

/**
 * pard
 *
 * @author guodong
 */
public class PardClient
{
    private final Socket socket;
    private final ObjectInputStream inputStream;
    private final BufferedWriter outWriter;
    private final Scanner scanner;

    public PardClient(String host, int port) throws IOException
    {
        this.socket = new Socket(host, port);
        this.inputStream = new ObjectInputStream(socket.getInputStream());
        this.outWriter = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
        this.scanner = new Scanner(System.in);
    }

    public void run()
    {
        while (true) {
            System.out.println("Welcome to Pard.");
            System.out.println("pard>");
            String line = scanner.nextLine();
            if (line.equalsIgnoreCase("QUIT") || line.equalsIgnoreCase("EXIT")) {
                break;
            }
            else {
                try {
                    String[] queries = line.split(";");
                    for (String q : queries)
                    {
                        outWriter.write(q);
                        outWriter.newLine();
                        outWriter.flush();
                    }
                }
                catch (IOException e) {
                    e.printStackTrace();
                    break;
                }
            }
        }
        System.out.println("Bye Pard");
        System.exit(0);
    }
}
