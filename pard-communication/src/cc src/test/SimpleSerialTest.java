package test;

import transfer.Attr;
import transfer.AttrType;
import transfer.TableSchema;

import java.io.*;
import java.util.ArrayList;

/**
 * Serializable obj <--> file
 */
public class SimpleSerialTest {
    public static void main(String[] args) throws Exception {

//construct table
        TableSchema ts=TestObj.tableSchema1();

        System.out.println("original ts:");
        System.out.println(ts.toString());

        //serialize to file
        String readPath = System.getProperty("user.dir") + File.separatorChar
                + "data" + File.separatorChar + "ts.out";

        ObjectOutputStream oout = new ObjectOutputStream(new FileOutputStream(new File(readPath)));
        oout.writeObject(ts);
        oout.close();



        ObjectInputStream oin = new ObjectInputStream(new FileInputStream(new File(readPath)));
        Object newTs = oin.readObject(); // 没有强制转换到table schema类型
        oin.close();
        System.out.println("\n\n new ts:");
        System.out.println(newTs);
    }
}
