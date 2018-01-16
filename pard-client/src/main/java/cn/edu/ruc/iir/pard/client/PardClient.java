package cn.edu.ruc.iir.pard.client;

import cn.edu.ruc.iir.pard.catalog.Column;
import cn.edu.ruc.iir.pard.commons.memory.Row;
import cn.edu.ruc.iir.pard.commons.utils.RowConstructor;
import cn.edu.ruc.iir.pard.executor.connector.PardResultSet;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.util.ArrayList;
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
                                List<Column> columns = resultSet.getSchema();
                                System.out.println(resultSet.toString());
                                final List<String> colNames = new ArrayList<>();
                                final List<Integer> colTypes = new ArrayList<>();
                                columns.forEach(c -> {
                                    colNames.add(c.getColumnName());
                                    colTypes.add(c.getDataType());
                                });
                                Object[] header = colNames.toArray();
                                String[] tableHeader = new String[header.length];
                                for (int i = 0; i < tableHeader.length; i++) {
                                    tableHeader[i] = (String) header[i];
                                }
                                PrettyTable pretty = new PrettyTable(tableHeader);
                                int counter = 0;
                                List<Row> rows = resultSet.getRows();
                                for (Row row : rows) {
                                    String temp = RowConstructor.printRow(row, colTypes);
                                    String[] r = temp.split("\t");
                                    pretty.addRow(r);
                                    counter++;
                                }
                                System.out.println(pretty);
                                System.out.println("Selected " + counter + " tuples");
                                System.out.println("Execution time: " + ((double) resultSet.getExecutionTime()) / 1000 + "s");
                                if (resultSet.getSemanticErrmsg() != null) {
                                    System.err.println("Semantic Status:" + resultSet.getSemanticErrmsg());
                                    System.out.println("Semantic Status:" + resultSet.getSemanticErrmsg());
                                }
                            }
                            else {
                                System.err.println(resultSet.getStatus().toString());
                                if (resultSet.getSemanticErrmsg() != null) {
                                    System.err.println(resultSet.getSemanticErrmsg());
                                    System.out.println(resultSet.getSemanticErrmsg());
                                }
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
