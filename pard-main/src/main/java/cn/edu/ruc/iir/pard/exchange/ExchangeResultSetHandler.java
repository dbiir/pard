package cn.edu.ruc.iir.pard.exchange;

import cn.edu.ruc.iir.pard.executor.connector.NextRSTask;
import cn.edu.ruc.iir.pard.executor.connector.PardResultSet;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * pard
 *
 * @author guodong
 */
public class ExchangeResultSetHandler
        extends ChannelInboundHandlerAdapter
{
    private final ConcurrentLinkedQueue<PardResultSet> resultSets;

    public ExchangeResultSetHandler(ConcurrentLinkedQueue<PardResultSet> resultSets)
    {
        this.resultSets = resultSets;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg)
    {
        PardResultSet resultSet = (PardResultSet) msg;
        System.out.println(resultSet);
        if (resultSet.getStatus() == PardResultSet.ResultStatus.EOR) {
            System.out.println("Get eor, close and return");
            if (ctx.channel().isOpen()) {
                ctx.close();
            }
        }
        else {
            ctx.write(new NextRSTask("site0"));
        }
        System.out.println("Add resultset");
        resultSets.offer(resultSet);
        System.out.println("Added resultset");
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
    {
        System.out.println("Exception caught");
        cause.printStackTrace();
        ctx.close();
    }
}
