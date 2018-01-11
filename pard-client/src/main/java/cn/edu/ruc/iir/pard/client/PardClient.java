package cn.edu.ruc.iir.pard.client;

import cn.edu.ruc.iir.pard.catalog.Column;
import cn.edu.ruc.iir.pard.commons.memory.Row;
import cn.edu.ruc.iir.pard.commons.utils.DataType;
import cn.edu.ruc.iir.pard.commons.utils.RowConstructor;
import cn.edu.ruc.iir.pard.executor.connector.PardResultSet;
import javafx.beans.binding.ObjectExpression;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
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

    /*
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
                                String header = Arrays.toString(colNames.toArray());
                                System.out.println(header);
                                for (int i = 0; i < header.length(); i++) {
                                    System.out.print("-");
                                }
                                System.out.print("\n");
                                List<Row> rows = resultSet.getRows();
                                for (Row row : rows) {
                                    System.out.println(RowConstructor.printRow(row, colTypes));
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
*/
    public PardClient()
    {
        this.inputStream = null;
        this.outWriter = new BufferedWriter(new OutputStreamWriter(System.out));
        this.scanner = new Scanner(System.in);
    }

    public void writeOut()
    {
        try {
            this.outWriter.write("hello world");
            this.outWriter.newLine();
            this.outWriter.flush();
        } catch (IOException e){
            e.printStackTrace();
        }
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
                        //Object obj = inputStream.readObject();
                        PardResultSet prs = new PardResultSet(PardResultSet.ResultStatus.OK);

                        List<Column> columns0 = new ArrayList<>();
                        Column col0 = new Column();
                        col0.setDataType(DataType.CHAR.getType());
                        col0.setLen(20);
                        col0.setColumnName("name");
                        Column col1 = new Column();
                        col1.setDataType(DataType.INT.getType());
                        col1.setColumnName("id");
                        col1.setKey(1);
                        Column col2 = new Column();
                        col2.setDataType(DataType.CHAR.getType());
                        col2.setLen(50);
                        col2.setColumnName("alma mater");
                        Column col3 = new Column();
                        col3.setDataType(DataType.FLOAT.getType());
                        col3.setLen(20);
                        col3.setColumnName("score");
                        columns0.add(col0);
                        columns0.add(col1);
                        columns0.add(col2);
                        columns0.add(col3);
                        prs.setSchema(columns0);

                        Object obj = prs;

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
                                //String header = Arrays.toString(colNames.toArray());
                                Object [] header = colNames.toArray();

                                for(Object s: header) {

                                    System.out.print("\t" + s + "\t");
                                    System.out.print("|");
                                }
                                System.out.println();
                                for (int i = 0; i < header.length; ｉ ++) {
                                    
                                }
                                /*
                                System.out.println(header);
                                for (int i = 0; i < header.length(); i++) {
                                    System.out.print("-");
                                }
                                */
                                System.out.print("\n");
                                List<Row> rows = resultSet.getRows();
                                for (Row row : rows) {
                                    System.out.println(RowConstructor.printRow(row, colTypes));
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
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        System.out.println("Bye Pard");
        System.exit(0);
    }

    public static void main(String[] args)
    {
        /*
        if (args.length != 2) {
            System.out.println("PardClient <host> <port>");
            System.exit(-1);
        }
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        System.out.println("Connecting to " + host + ":" + port);
        try {printstream 初始化
            PardClient client = new PardClient(host, port);
            client.run();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        */

        test();

    }

    private static void test()
    {
        try {
            PardClient pc = new PardClient();
            //pc.writeOut();
            pc.run();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
