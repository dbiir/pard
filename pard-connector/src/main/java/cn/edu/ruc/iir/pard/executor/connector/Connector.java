package cn.edu.ruc.iir.pard.executor.connector;

import cn.edu.ruc.iir.pard.scheduler.Task;

/**
 * pard
 *
 * @author guodong
 */
public interface Connector
{
    void execute(Task task);

    void close();
}
