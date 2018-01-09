package cn.edu.ruc.iir.pard.exchange;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.DefaultFileRegion;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.logging.Logger;

/**
 * pard
 *
 * @author guodong
 */
public class ExchangeFileSendHandler
        extends ChannelInboundHandlerAdapter
{
    private final Logger logger = Logger.getLogger(ExchangeFileSendHandler.class.getName());
    private final String path;

    public ExchangeFileSendHandler(String path)
    {
        this.path = path;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx)
    {
        logger.info("Channel is active, sending file...");
        ctx.writeAndFlush("HEADER: " + path + "\n");
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg)
    {
        String message = (String) msg;
        if (message.equalsIgnoreCase("OKHEADER")) {
            RandomAccessFile raf = null;
            long length = -1;
            try {
                raf = new RandomAccessFile(path, "r");
                length = raf.length();
            }
            catch (IOException e) {
                e.printStackTrace();
                return;
            }
            finally {
                if (length < 0 && raf != null) {
                    try {
                        raf.close();
                    }
                    catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }

            ChannelFuture f = ctx.writeAndFlush(new DefaultFileRegion(raf.getChannel(), 0, length));
            f.addListener((ChannelFutureListener) future -> {
                ChannelFuture lastFuture = ctx.writeAndFlush("OKDONE\n");
                lastFuture.addListener((ChannelFutureListener) future1 -> ctx.close());
            });
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
    {
        cause.printStackTrace();

        if (ctx.channel().isActive()) {
            ctx.writeAndFlush("ERR: " +
                    cause.getClass().getSimpleName() + ": " +
                    cause.getMessage() + '\n').addListener(ChannelFutureListener.CLOSE);
        }
    }
}
