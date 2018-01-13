package cn.edu.ruc.iir.pard.client;

import cn.edu.ruc.iir.pard.catalog.Column;
import cn.edu.ruc.iir.pard.commons.memory.Row;
import cn.edu.ruc.iir.pard.commons.utils.DataType;
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
                                //pretty.printLargeDataSets();
                                //pretty.printLargeDataSetsOneByOne();
                                System.out.println("Selected " + counter + " tuples");
                                System.out.println("Execution time: " + ((double) resultSet.getExecutionTime()) / 1000 + "s");
                                if (resultSet.getSemanticErrmsg() != null) {
                                    System.err.println("Semantic Status:" + resultSet.getSemanticErrmsg());
                                }
                            }
                            else {
                                System.err.println(resultSet.getStatus().toString());
                                if (resultSet.getSemanticErrmsg() != null) {
                                    System.err.println(resultSet.getSemanticErrmsg());
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


    public PardClient()
    {
        this.inputStream = null;
        this.outWriter = new BufferedWriter(new OutputStreamWriter(System.out));
        this.scanner = new Scanner(System.in);
    }
    public void testrun()
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
                        String temp = null;
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
                                //List<Row> rows = new ArrayList<Row>();
                                RowConstructor rc1 = new RowConstructor();
                                rc1.appendString("TOMTOMTOM");
                                rc1.appendInt(121345);
                                rc1.appendString("RUCRUCRUCRUCRUCRUC");
                                rc1.appendFloat(78.2f);
                                Row row1 = rc1.build();
                                temp = rc1.printRow(row1, colTypes);
                                String[] r1 = temp.substring(0, temp.length() - 1).split("\t");
                                RowConstructor rc2 = new RowConstructor();
                                rc2.appendString("TOMTOMTOMTOMTOMTOMTOMTOMTOM");
                                rc2.appendInt(1213415565);
                                rc2.appendString("RUCRUCRUCRUCRUCRUCRUCRUCRUCRUCRUCRUC");
                                rc2.appendFloat(78.248481f);
                                Row row2 = rc2.build();
                                temp = rc2.printRow(row2, colTypes);
                                String[] r2 = temp.substring(0, temp.length() - 1).split("\t");
                                RowConstructor rc3 = new RowConstructor();
                                rc3.appendString("TOM");
                                rc3.appendInt(1214);
                                rc3.appendString("RUC");
                                rc3.appendFloat(78.248481f);
                                Row row3 = rc3.build();
                                temp = rc3.printRow(row3, colTypes);
                                String[] r3 = temp.substring(0, temp.length() - 1).split("\t");
                                RowConstructor rc4 = new RowConstructor();
                                rc4.appendString("T");
                                rc4.appendInt(121345);
                                rc4.appendString("R");
                                rc4.appendFloat(78.2f);
                                Row row4 = rc4.build();
                                temp = rc4.printRow(row1, colTypes);
                                String[] r4 = temp.substring(0, temp.length() - 1).split("\t");
                                RowConstructor rc5 = new RowConstructor();
                                rc5.appendString("OM");
                                rc5.appendInt(1213415565);
                                rc5.appendString("UCR");
                                rc5.appendFloat(78.248481f);
                                Row row5 = rc5.build();
                                temp = rc5.printRow(row5, colTypes);
                                String[] r5 = temp.substring(0, temp.length() - 1).split("\t");
                                RowConstructor rc6 = new RowConstructor();
                                rc6.appendString("OMT");
                                rc6.appendInt(1214);
                                rc6.appendString("CRU");
                                rc6.appendFloat(78.248481f);
                                Row row6 = rc6.build();
                                temp = rc6.printRow(row3, colTypes);
                                String[] r6 = temp.substring(0, temp.length() - 1).split("\t");


                                for (int i = 0; i < 200000; i++) {
                                    RowConstructor rc1 = new RowConstructor();
                                    rc1.appendString("TOMTOMTOM");
                                    rc1.appendInt(121345);
                                    rc1.appendString("RUCRUCRUCRUCRUCRUC");
                                    rc1.appendFloat(78.2f);
                                    Row row1 = rc1.build();
                                    temp = rc1.printRow(row1, colTypes);
                                    String[] r1 = temp.substring(0, temp.length() - 1).split("\t");
                                    RowConstructor rc2 = new RowConstructor();
                                    rc2.appendString("TOMTOMTOMTOMTOMTOMTOMTOMTOM");
                                    rc2.appendInt(1213415565);
                                    rc2.appendString("RUCRUCRUCRUCRUCRUCRUCRUCRUCRUCRUCRUC");
                                    rc2.appendFloat(78.248481f);
                                    Row row2 = rc2.build();
                                    temp = rc2.printRow(row2, colTypes);
                                    String[] r2 = temp.substring(0, temp.length() - 1).split("\t");
                                    RowConstructor rc3 = new RowConstructor();
                                    rc3.appendString("TOM");
                                    rc3.appendInt(1214);
                                    rc3.appendString("RUC");
                                    rc3.appendFloat(78.248481f);
                                    Row row3 = rc3.build();
                                    temp = rc3.printRow(row3, colTypes);
                                    String[] r3 = temp.substring(0, temp.length() - 1).split("\t");
                                    RowConstructor rc4 = new RowConstructor();
                                    rc4.appendString("T");
                                    rc4.appendInt(121345);
                                    rc4.appendString("R");
                                    rc4.appendFloat(78.2f);
                                    Row row4 = rc4.build();
                                    temp = rc4.printRow(row1, colTypes);
                                    String[] r4 = temp.substring(0, temp.length() - 1).split("\t");
                                    RowConstructor rc5 = new RowConstructor();
                                    rc5.appendString("OM");
                                    rc5.appendInt(1213415565);
                                    rc5.appendString("UCR");
                                    rc5.appendFloat(78.248481f);
                                    Row row5 = rc5.build();
                                    temp = rc5.printRow(row5, colTypes);
                                    String[] r5 = temp.substring(0, temp.length() - 1).split("\t");
                                    RowConstructor rc6 = new RowConstructor();
                                    rc6.appendString("OMT");
                                    rc6.appendInt(1214);
                                    rc6.appendString("CRU");
                                    rc6.appendFloat(78.248481f);
                                    Row row6 = rc6.build();
                                    temp = rc6.printRow(row3, colTypes);
                                    String[] r6 = temp.substring(0, temp.length() - 1).split("\t");
                                    RowConstructor rc7 = new RowConstructor();
                                    rc7.appendString("hebe");
                                    rc7.appendInt(7899);
                                    rc7.appendString("irc");
                                    rc7.appendFloat(784.5f);
                                    Row row7 = rc7.build();
                                    temp = rc7.printRow(row7, colTypes);
                                    String[] r7 = temp.substring(0, temp.length() - 1).split("\t");
                                    pretty.addRow(r1);
                                    pretty.addRow(r2);
                                    pretty.addRow(r3);
                                    pretty.addRow(r4);
                                    pretty.addRow(r5);
                                    pretty.addRow(r6);
                                    pretty.addRow(r7);
                                }
                                long st = System.currentTimeMillis();
                                System.out.println(pretty);
                                //pretty.printLargeDataSets();
                                //pretty.printLargeDataSetsOneByOne();
                                long et = System.currentTimeMillis();
                                System.out.println("TIME " + (et - st));
                                System.out.println(pretty.rowSize());
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
    private static void testPrettyTable()
    {
        PrettyTable table = new PrettyTable("Firstname", "Lastname", "Email", "Phone");
        table.addRow("John", "Doe", "johndoe@nothing.com", "+2137999999");
        table.addRow("Jane", "Doe", "janedoe@nothin.com", "+2137999999");
        System.out.println(table);
        PardClient pc = new PardClient();
        pc.testrun();
    }


    public static void main(String[] args)
    {
        testPrettyTable();
        /*
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
        */
    }
}
