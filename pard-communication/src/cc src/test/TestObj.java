package test;

import transfer.Attr;
import transfer.AttrType;
import transfer.TableSchema;

public class TestObj {
    /**
     *
     * @return an instance of TableSchema, for test.
     */
    public static TableSchema tableSchema1(){
        Attr a0 = new Attr(AttrType.INT,4);
        Attr a1 = new Attr(AttrType.STR,20*2);//char(20)
        Attr[] al=new Attr[2];
        al[0]=a0; al[1]=a1;
        String[] sss = new String[]{"sal","title"};
        TableSchema ts = new TableSchema(al, sss, 5);
        return ts;
    }
}
