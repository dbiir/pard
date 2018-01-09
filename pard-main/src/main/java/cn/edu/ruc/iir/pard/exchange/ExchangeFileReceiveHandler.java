package cn.edu.ruc.iir.pard.exchange;

import cn.edu.ruc.iir.pard.executor.PardTaskExecutor;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.logging.Logger;

/**
 * pard
 *
 * @author guodong
 */
public class ExchangeFileReceiveHandler
        extends ChannelInboundHandlerAdapter
{
    private final Logger logger = Logger.getLogger(ExchangeFileReceiveHandler.class.getName());
    private final PardTaskExecutor executor;
    private String schema = null;
    private String table = null;
    private String path = "/dev/shm/tmp" + String.valueOf(System.currentTimeMillis());
    private BufferedWriter writer = null;

    public ExchangeFileReceiveHandler(PardTaskExecutor executor)
    {
        this.executor = executor;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx)
    {
        logger.info("Channel is active, ready to receive file");
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg)
    {
        try {
            if (msg instanceof String) {
                String message = (String) msg;
                if (message.startsWith("HEADER: ")) {
                    logger.info("File header: " + message);
                    String[] eles = message.split(",");
                    this.schema = eles[0];
                    this.table = eles[1];
                    this.path = eles[2];
                    this.path = path + ".copy";
                    System.out.println(path);
                    writer = new BufferedWriter(new FileWriter(path));
                    ctx.writeAndFlush("OKHEADER\n");
                }
                else if (message.equalsIgnoreCase("OKDONE")) {
                    logger.info("File writer close");
                    if (writer != null) {
                        writer.close();
                    }
                    ctx.close();
                }
                else {
                    if (writer != null) {
                        writer.write(message + "\n");
                    }
                }
            }
        }
        catch (IOException e) {
            e.printStackTrace();
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
