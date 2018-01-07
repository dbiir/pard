package cn.edu.ruc.iir.pard.exchange;

import cn.edu.ruc.iir.pard.executor.connector.Block;
import cn.edu.ruc.iir.pard.executor.connector.Task;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * pard
 *
 * @author guodong
 */
public class ExchangeBlockHandler
        extends ChannelInboundHandlerAdapter
{
    private final Task task;
    private final ConcurrentLinkedQueue<Block> blocks;
    private final Logger logger = Logger.getLogger(ExchangeBlockHandler.class.getName());

    public ExchangeBlockHandler(Task task, ConcurrentLinkedQueue<Block> blocks)
    {
        this.task = task;
        this.blocks = blocks;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg)
    {
        if (msg instanceof Block) {
            Block block = (Block) msg;
            blocks.add(block);
            if (block.isSequenceHasNext()) {
                ctx.write(task);
            }
            else {
                ctx.close();
            }
        }
        else {
            logger.log(Level.WARNING, "Exchange block handler received a message which is not a block");
            ctx.close();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
    {
        System.out.println("Exception caught");
        cause.printStackTrace();
        ctx.close();
    }
}
