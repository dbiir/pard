package cn.edu.ruc.iir.pard.exchange;

import cn.edu.ruc.iir.pard.commons.memory.Block;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * pard
 *
 * @author guodong
 */
public class ExchangeBlockHandler
        extends ChannelInboundHandlerAdapter
{
    private final ConcurrentLinkedQueue<Block> blocks;

    public ExchangeBlockHandler(ConcurrentLinkedQueue<Block> blocks)
    {
        this.blocks = blocks;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg)
    {
        Block block = (Block) msg;
        blocks.add(block);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
    {
        System.out.println("Exception caught");
        cause.printStackTrace();
        ctx.close();
    }
}
