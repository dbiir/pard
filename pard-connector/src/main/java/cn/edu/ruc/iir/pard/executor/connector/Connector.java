package cn.edu.ruc.iir.pard.executor.connector;

/**
 * pard
 *
 * @author guodong
 */
public interface Connector
{
    PardResultSet execute(Task task);

    void close();
}
