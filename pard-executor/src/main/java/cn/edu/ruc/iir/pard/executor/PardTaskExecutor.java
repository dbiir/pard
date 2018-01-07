package cn.edu.ruc.iir.pard.executor;

import cn.edu.ruc.iir.pard.executor.connector.Connector;
import cn.edu.ruc.iir.pard.executor.connector.PardResultSet;
import cn.edu.ruc.iir.pard.executor.connector.Task;

/**
 * pard
 *
 * @author guodong
 */
public class PardTaskExecutor
        implements Runnable
{
    private final Connector connector;
    private final Task task;

    public PardTaskExecutor(Connector connector)
    {
        this.connector = connector;
        this.task = null;
    }
    public PardTaskExecutor(Connector connector, Task task)
    {
        this.connector = connector;
        this.task = task;
    }

    public PardResultSet execute(Task task)
    {
        return connector.execute(task);
    }

    @Override
    public void run()
    {
    }
}
