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
        Attr a0 = new Attr(AttrType.INT,4);
        Attr a1 = new Attr(AttrType.STR,20*2);//char(20)
        Attr[] al=new Attr[2];
        al[0]=a0; al[1]=a1;
        String[] sss=new String[]{"sal","title"};
        TableSchema ts=new TableSchema(al, sss, 5);
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
