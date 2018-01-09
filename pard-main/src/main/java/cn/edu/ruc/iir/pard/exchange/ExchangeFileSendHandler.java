package cn.edu.ruc.iir.pard.exchange;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.DefaultFileRegion;

import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * pard
 *
 * @author guodong
 */
public class ExchangeFileSendHandler
        extends ChannelInboundHandlerAdapter
{
    private final String schema;
    private final String table;
    private final String path;

    public ExchangeFileSendHandler(String schema, String table, String path)
    {
        this.schema = schema;
        this.table = table;
        this.path = path;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx)
    {
        System.out.println("Channel is active, sending file...");
        ctx.writeAndFlush("HEADER: " + schema + "," + table + "," + path + "\n");
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

            ctx.writeAndFlush(new DefaultFileRegion(raf.getChannel(), 0, length));
            ctx.writeAndFlush("\n");
            ChannelFuture f = ctx.writeAndFlush("OKDONE\n");
            f.addListener((ChannelFutureListener) future -> ctx.close());
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
