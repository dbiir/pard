package cn.edu.ruc.iir.pard.communication.rpc;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import static io.netty.buffer.Unpooled.wrappedBuffer;


public class ByteServerHandler  extends ChannelInboundHandlerAdapter {

    byte[] data;//data to be sent


    void setData(byte[] data){
        this.data=data;
    }

    /**
     * call setData() to set the data.
     * @param ctx
     */
    @Override
    public void channelActive(final ChannelHandlerContext ctx) { // (1)

        final ByteBuf data1 = wrappedBuffer(data);

        final ChannelFuture f = ctx.writeAndFlush(data1); // (3)
        f.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) {
                assert f == future;
                ctx.close();
            }
        }); // (4)
//        f.addListener(ChannelFutureListener.CLOSE);


    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }
}
