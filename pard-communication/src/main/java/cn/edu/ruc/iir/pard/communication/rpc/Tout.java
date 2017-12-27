package cn.edu.ruc.iir.pard.communication.rpc;

import transfer.TableSchema;

import static transfer.SerializationUtils.serialize;

public class Tout {

    private static  ByteServer bs;

    public Tout() {
        int    port = 8081;
        bs=new ByteServer(port);

    }

    public static ByteServer getBs() {
        return bs;
    }

    public static void main(String[] args) throws Exception{
       Tout to=new Tout();
       ByteServer bs=to.getBs();
        TableSchema ts= TestObj.tableSchema1();
        System.out.println("server  ts:");
        System.out.println(ts);
        byte[] data = transfer.SerializationUtils.serialize(ts);
        bs.setData(data);
        bs.run();       //阻塞在这里了
        System.out.println("11111111111");


        Tout1 t1=new Tout1();
        t1.setBs(bs);
        t1.test();
    }


}
