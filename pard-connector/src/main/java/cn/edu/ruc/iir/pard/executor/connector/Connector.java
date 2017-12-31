package cn.edu.ruc.iir.pard.executor.connector;

import cn.edu.ruc.iir.pard.commons.utils.PardResultSet;

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
