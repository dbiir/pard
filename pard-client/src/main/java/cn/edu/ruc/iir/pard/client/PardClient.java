package cn.edu.ruc.iir.pard.client;

import cn.edu.ruc.iir.pard.utils.PardResultSet;

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
    private final ObjectInputStream inputStream;
    private final BufferedWriter outWriter;
    private final Scanner scanner;

    public PardClient(String host, int port) throws IOException
    {
        Socket socket = new Socket(host, port);
        this.inputStream = new ObjectInputStream(socket.getInputStream());
        this.outWriter = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
        this.scanner = new Scanner(System.in);
    }

    public void run()
    {
        System.out.println("Welcome to Pard.");
        System.out.println("pard>");
        while (true) {
            String line = scanner.nextLine();
            if (line.equalsIgnoreCase("QUIT") || line.equalsIgnoreCase("EXIT")) {
                break;
            }
            else {
                try {
                    String[] queries = line.split(";");
                    for (String q : queries) {
                        outWriter.write(q);
                        outWriter.newLine();
                        outWriter.flush();
                        PardResultSet resultSet = (PardResultSet) inputStream.readObject();
                        switch (resultSet.getStatus()) {
                            case PARSING_ERR:
                                System.out.println("Parsing error");
                                break;
                            case EXEC_ERR:
                                System.out.println("Execution error");
                                break;
                            case OK:
                                System.out.println(resultSet.toString());
                                break;
                        }
                    }
                }
                catch (IOException e) {
                    e.printStackTrace();
                    break;
                }
                catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
            }
        }
        System.out.println("Bye Pard");
        System.exit(0);
    }

    public static void main(String[] args)
    {
        if (args.length != 2) {
            System.out.println("PardClient <host> <port>");
            System.exit(-1);
        }
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        System.out.println("Connecting to " + host + ":" + port);
        try {
            PardClient client = new PardClient(host, port);
            client.run();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
}
