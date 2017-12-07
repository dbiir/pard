package test;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import test.TestObj;
import transfer.TableSchema;

import static io.netty.buffer.Unpooled.wrappedBuffer;

public class ByteServerHandler  extends ChannelInboundHandlerAdapter {

    @Override
    public void channelActive(final ChannelHandlerContext ctx) { // (1)
        TableSchema ts= TestObj.tableSchema1();
        System.out.println("server sent ts:");
        System.out.println(ts);
        byte[] data = transfer.SerializationUtils.serialize(ts);

        //尝试不用 ByteBuf 收发
        final ByteBuf data1 = wrappedBuffer(data);

        final ChannelFuture f = ctx.writeAndFlush(data1); // (3)
        f.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) {
                assert f == future;
                ctx.close();
            }
        }); // (4)
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }
}
