package cn.edu.ruc.iir.pard.exchange;

import cn.edu.ruc.iir.pard.executor.PardTaskExecutor;
import cn.edu.ruc.iir.pard.executor.connector.Block;
import cn.edu.ruc.iir.pard.executor.connector.Task;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * pard
 *
 * @author guodong
 */
@ChannelHandler.Sharable
public class ExchangeTaskHandler
        extends ChannelInboundHandlerAdapter
{
    private final Logger logger = Logger.getLogger(ExchangeTaskHandler.class.getName());
    private final PardTaskExecutor executor;

    public ExchangeTaskHandler(PardTaskExecutor executor)
    {
        this.executor = executor;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg)
    {
        if (msg instanceof Task) {
            Task task = (Task) msg;
            Block block = executor.execute(task);
            if (block.isSequenceHasNext()) {
                ctx.write(block);
            }
            else {
                ChannelFuture f = ctx.write(block);
                f.addListener(ChannelFutureListener.CLOSE);
            }
        }
        else {
            logger.log(Level.WARNING, "Exchange task handler received a message which is not a task");
            ctx.close();
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
