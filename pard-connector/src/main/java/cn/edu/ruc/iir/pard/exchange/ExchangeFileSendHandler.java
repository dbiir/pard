package cn.edu.ruc.iir.pard.exchange;

import cn.edu.ruc.iir.pard.executor.connector.PardResultSet;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.DefaultFileRegion;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.ConcurrentLinkedQueue;
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
    private final String schema;
    private final String table;
    private final String taskId;
    private final ConcurrentLinkedQueue<PardResultSet> resultSets;

    public ExchangeFileSendHandler(String path, String schema, String table, String taskId,
                                   ConcurrentLinkedQueue<PardResultSet> resultSets)
    {
        this.path = path;
        this.schema = schema;
        this.table = table;
        this.taskId = taskId;
        this.resultSets = resultSets;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx)
    {
        logger.info("Channel is active, sending file...");
        ctx.writeAndFlush("HEADER:" + schema + ":" + table + "\n");
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws IOException
    {
        String message = (String) msg;
        if (message.equalsIgnoreCase("OKHEADER")) {
            RandomAccessFile raf = null;
            long length = -1;
            try {
                raf = new RandomAccessFile(path, "r");
                length = raf.length();
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

            if (path.endsWith("SENDDATA")) { // SEND DATA TASK
                ChannelFuture f = ctx.writeAndFlush(new DefaultFileRegion(raf.getChannel(), 0, length));
                f.addListener((ChannelFutureListener) future -> ctx.writeAndFlush("OKDONESENDDATA\n"));
            }
            else { // LOAD TASK
                ChannelFuture f = ctx.writeAndFlush(new DefaultFileRegion(raf.getChannel(), 0, length));
                f.addListener((ChannelFutureListener) future -> ctx.writeAndFlush("OKDONE\n"));
            }
        }
        if (message.equalsIgnoreCase("OK")) {
            PardResultSet resultSet = new PardResultSet(PardResultSet.ResultStatus.OK);
            resultSet.setTaskId(taskId);
            resultSets.add(resultSet);
            logger.info("Task " + taskId + " execute ok");
            ctx.close();
        }
        if (message.equalsIgnoreCase("EXECUTION_ERROR")) {
            PardResultSet resultSet = new PardResultSet(PardResultSet.ResultStatus.EXECUTING_ERR);
            resultSet.setTaskId(taskId);
            resultSets.add(resultSet);
            logger.info("Task " + taskId + " execute error");
            ctx.close();
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
