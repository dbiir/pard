package cn.edu.ruc.iir.pard.server;

import cn.edu.ruc.iir.pard.commons.utils.PardResultSet;
import cn.edu.ruc.iir.pard.executor.connector.EORTask;
import cn.edu.ruc.iir.pard.executor.connector.Task;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

/**
 * pard
 *
 * @author guodong
 */
public class PardNettyServerHandler
        extends SimpleChannelInboundHandler<Task>
{
    @Override
    public void channelActive(final ChannelHandlerContext ctx)
    {
        System.out.println("Server channel active!");
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Task msg)
    {
        System.out.println(msg.getSite() + ": " + msg.getTaskId());
        if (msg instanceof EORTask) {
            ChannelFuture future = ctx.write(new PardResultSet(PardResultSet.ResultStatus.EOR));
            future.addListener(ChannelFutureListener.CLOSE);
        }
        else {
            ctx.write(new PardResultSet(PardResultSet.ResultStatus.OK));
            ctx.write(new PardResultSet(PardResultSet.ResultStatus.EOR));
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
        cause.printStackTrace();
        ctx.close();
    }
}
