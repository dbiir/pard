package cn.edu.ruc.iir.pard.exchange;

import cn.edu.ruc.iir.pard.executor.connector.Connector;
import cn.edu.ruc.iir.pard.executor.connector.PardResultSet;
import cn.edu.ruc.iir.pard.executor.connector.Task;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * pard
 *
 * @author guodong
 */
@ChannelHandler.Sharable
public class ExchangeTaskHandler
        extends ChannelInboundHandlerAdapter
{
    private final Connector connector;

    public ExchangeTaskHandler(Connector connector)
    {
        this.connector = connector;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg)
    {
        if (msg instanceof Task) {
            for (int i = 0; i < 10; i++) {
                if (ctx.channel().isWritable()) {
                    Task task = (Task) msg;
                    System.out.println(task);
                    ctx.write(PardResultSet.okResultSet);
                }
                else {
                    System.out.println("Not writable");
                }
            }
            ChannelFuture f = ctx.write(PardResultSet.eorResultSet);
            f.addListener(ChannelFutureListener.CLOSE);
        }
        else {
            System.out.println("Error task");
        }
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx)
    {
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
    {
        System.out.println("Exception caught");
        cause.printStackTrace();
        ctx.close();
    }
}
