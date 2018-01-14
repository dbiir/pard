package cn.edu.ruc.iir.pard.exchange;

import cn.edu.ruc.iir.pard.executor.PardTaskExecutor;
import cn.edu.ruc.iir.pard.executor.connector.Block;
import cn.edu.ruc.iir.pard.executor.connector.JoinTask;
import cn.edu.ruc.iir.pard.executor.connector.Task;
import cn.edu.ruc.iir.pard.executor.connector.UnionTask;
import cn.edu.ruc.iir.pard.scheduler.TaskScheduler;
import cn.edu.ruc.iir.pard.scheduler.TaskState;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import net.sf.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * pard
 *
 * @author guodong
 */
public class ExchangeTaskHandler
        extends ChannelInboundHandlerAdapter
{
    private final Logger logger = Logger.getLogger(ExchangeTaskHandler.class.getName());
    private final PardTaskExecutor executor;
    private final Map<String, TaskState> taskMap;
    private final Map<String, List<String>> subTaskMap;
    private final TaskScheduler taskScheduler;
    public ExchangeTaskHandler(PardTaskExecutor executor)
    {
        this.executor = executor;
        this.taskMap = new HashMap<String, TaskState>();
        this.subTaskMap = new HashMap<>();
        this.taskScheduler = TaskScheduler.INSTANCE();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg)
    {
        if (msg instanceof Task && !(msg instanceof UnionTask) && !(msg instanceof JoinTask)) {
            Task task = (Task) msg;
            Block block = executor.executeQuery(task);
            if (block.isSequenceHasNext()) {
                ctx.write(block);
            }
            else {
                ChannelFuture f = ctx.write(block);
                f.addListener(ChannelFutureListener.CLOSE);
            }
        }
        else if (msg instanceof Task && msg instanceof UnionTask) {
            unionTask(ctx, (UnionTask) msg);
        }
        else {
            logger.log(Level.WARNING, "Exchange task handler received a message which is not a task");
            ctx.close();
        }
    }
    public void unionTask(ChannelHandlerContext ctx, UnionTask task)
    {
        //int p = 0;
        if (subTaskMap.get(task.getTaskId()) == null) {
            List<String> list = new ArrayList<String>();
            TaskState state = taskScheduler.executeQueryTask(task.getWaitTask(), true);
            list.add(task.getTaskId());
            subTaskMap.put(task.getTaskId(), list);
            taskMap.put(task.getTaskId(), state);
        }
        TaskState state = taskMap.get(task.getTaskId());
        boolean hasNext = true;
        ChannelFuture f = null;
        if (!state.isDone()) {
            logger.info("waiting more blocks in exchange task handlers.");
            logger.info("print task map:");
            for (String key : state.getTaskMap().keySet()) {
                logger.info("task map key " + key + JSONObject.fromObject(state.getTaskMap().get(key)).toString());
            }
            Block b = state.fetch();
            hasNext = b.isSequenceHasNext() || state.getTaskMap().size() > 1;
            b.setSequenceHasNext(hasNext);
            f = ctx.write(b);
        }
        if (!hasNext) {
            f.addListener(ChannelFutureListener.CLOSE);
            subTaskMap.put(task.getTaskId(), null);
            taskMap.put(task.getTaskId(), null);
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
