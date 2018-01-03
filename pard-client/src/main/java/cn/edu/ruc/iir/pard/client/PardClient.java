package cn.edu.ruc.iir.pard.client;

import cn.edu.ruc.iir.pard.commons.memory.Block;
import cn.edu.ruc.iir.pard.commons.memory.Row;
import cn.edu.ruc.iir.pard.commons.utils.DataType;
import cn.edu.ruc.iir.pard.commons.utils.PardResultSet;
import cn.edu.ruc.iir.pard.commons.utils.RowConstructor;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.util.Arrays;
import java.util.List;
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
        while (true) {
            System.out.print("pard>");
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
                        Object obj = inputStream.readObject();
                        if (obj instanceof PardResultSet) {
                            PardResultSet resultSet = (PardResultSet) obj;
                            if (resultSet.getStatus() == PardResultSet.ResultStatus.OK) {
                                System.out.println(resultSet.toString());
                                List<String> colNames;
                                List<DataType> colTypes;
                                while (resultSet.hasNext()) {
                                    Block block = resultSet.getNext();
                                    colNames = block.getColumnNames();
                                    String header = Arrays.toString(colNames.toArray());
                                    System.out.println(header);
                                    for (int i = 0; i < header.length(); i++) {
                                        System.out.print("-");
                                    }
                                    System.out.print("\n");
                                    colTypes = block.getColumnTypes();
                                    while (block.hasNext()) {
                                        Row row = block.getNext();
                                        System.out.println(RowConstructor.printRow(row, colTypes));
                                    }
                                }
                            }
                            else {
                                System.out.println(resultSet.getStatus().toString());
                            }
                        }
                        else {
                            System.out.println("Client receive unknown object");
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
