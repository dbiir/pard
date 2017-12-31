package cn.edu.ruc.iir.pard.server;

import cn.edu.ruc.iir.pard.commons.utils.PardResultSet;
import cn.edu.ruc.iir.pard.executor.connector.EORTask;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

/**
 * pard
 *
 * @author guodong
 */
public class PardNettyClientHandler
        extends SimpleChannelInboundHandler<PardResultSet>
{
    @Override
    public void channelActive(final ChannelHandlerContext ctx)
    {
        System.out.println("Client channel active!");
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, PardResultSet msg)
    {
        if (msg.getStatus() == PardResultSet.ResultStatus.EOR) {
            ctx.writeAndFlush(new EORTask("close"));
        }
        System.out.println("Result: " + msg.getStatus().toString());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
    {
        cause.printStackTrace();
        ctx.close();
    }
}
