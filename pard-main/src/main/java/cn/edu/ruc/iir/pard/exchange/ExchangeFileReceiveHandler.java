package cn.edu.ruc.iir.pard.exchange;

import cn.edu.ruc.iir.pard.executor.PardTaskExecutor;
import cn.edu.ruc.iir.pard.executor.connector.LoadTask;
import cn.edu.ruc.iir.pard.executor.connector.PardResultSet;
import cn.edu.ruc.iir.pard.executor.connector.Task;
import com.google.common.collect.ImmutableList;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.io.BufferedWriter;
import java.io.File;
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
    private String schema;
    private String table;
    private BufferedWriter writer = null;
    private String path = null;

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
                if (writer == null) {
                    this.path = "/dev/shm/tmp-" + String.valueOf(System.currentTimeMillis());
                    File file = new File(path);
                    file.createNewFile();
                    this.writer = new BufferedWriter(new FileWriter(file));
                }
                String message = (String) msg;
                if (message.startsWith("HEADER:")) {
                    logger.info("File header: " + message);
                    String[] messages = message.split(":");
                    this.schema = messages[1].trim();
                    this.table = messages[2].trim();
                    ctx.writeAndFlush("OKHEADER\n");
                }
                else if (message.equalsIgnoreCase("OKDONE")) {
                    logger.info("File writer close");
                    if (writer != null) {
                        writer.close();
                    }
                    Task task = new LoadTask(schema, table, ImmutableList.of(path));
                    PardResultSet resultSet = executor.executeStatus(task);
                    logger.info("File copy result: " + resultSet.getStatus().toString());
                    writer = null;
                    path = null;
                    ChannelFuture future = ctx.writeAndFlush(resultSet.getStatus().toString() + "\n");
                    future.addListener((ChannelFutureListener) f -> ctx.close());
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
