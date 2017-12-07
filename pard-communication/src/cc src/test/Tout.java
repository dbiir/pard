package test;

import io.netty.buffer.ByteBuf;
import transfer.Attr;
import transfer.AttrType;
import transfer.TableSchema;

import java.util.ArrayList;

import static transfer.SerializationUtils.serialize;

public class Tout {
    public static void main(String[] args) throws Exception{
        Attr a0 = new Attr(AttrType.INT,4);
        Attr a1 = new Attr(AttrType.STR,20*2);//char(20)
        Attr[] al=new Attr[2];
        al[0]=a0; al[1]=a1;
        String[] sss=new String[]{"sal","title"};
        TableSchema ts=new TableSchema(al, sss, 5);
//        ByteBuf buf=new ByteBuf() ;

        byte[] data = serialize(ts);

        //发送

    }
}
