package cn.edu.ruc.iir.pard.communication.rpc;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import static transfer.SerializationUtils.deserialize;

public class ByteClientHandler extends ChannelInboundHandlerAdapter {

    byte[] receivedData;



    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        ByteBuf m = (ByteBuf) msg; // (1)
        byte[] bytes = new byte[m.readableBytes()];
        m.readBytes(bytes);
        m.release();

        Object obj = deserialize(bytes); //TableSchema ts
        try {
            System.out.println("client received ts:");
            System.out.println(obj);
        } catch (Exception e){
            e.printStackTrace();
        }
        finally {
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }

}
