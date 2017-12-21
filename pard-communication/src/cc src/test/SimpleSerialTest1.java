package test;

import transfer.Attr;
import transfer.AttrType;
import transfer.TableSchema;

import java.io.*;

import static transfer.SerializationUtils.deserialize;
import static transfer.SerializationUtils.serialize;


/**
 * Serializable obj <--> byte[] (in memory)
 */
public class SimpleSerialTest1 {
    public static void main(String[] args) throws Exception {

//construct table
        TableSchema ts=TestObj.tableSchema1();
        System.out.println("original ts:");
        System.out.println(ts.toString());

        //serialize to file
        String readPath = System.getProperty("user.dir") + File.separatorChar
                + "data" + File.separatorChar + "ts.out1";

        ObjectOutputStream oout = new ObjectOutputStream(new FileOutputStream(new File(readPath)));
//        oout.writeObject(ts.toBytes());
//        byte[] data = serialize(ts);
        byte[] data = serialize(ts);
        oout.close();


        Object newTs = deserialize(data); // 没有强制转换到table schema类型
        System.out.println("\n\n new ts:");
        System.out.println(newTs);
    }


}
