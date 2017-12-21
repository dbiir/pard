package cn.edu.ruc.iir.pard.connector.postgresql;

import cn.edu.ruc.iir.pard.commons.config.PardUserConfiguration;
import cn.edu.ruc.iir.pard.executor.connector.Connector;
import cn.edu.ruc.iir.pard.scheduler.Task;

/**
 * pard
 *
 * @author guodong
 */
public class PostgresConnector
        implements Connector
{
    private final ConnectionPool connectionPool;

    public PostgresConnector()
    {
        PardUserConfiguration configuration = PardUserConfiguration.INSTANCE();
        connectionPool = new ConnectionPool(
                configuration.getConnectorDriver(),
                configuration.getConnectorHost(),
                configuration.getConnectorUser(),
                configuration.getConnectorPassword());
    }

    @Override
    public void execute(Task task)
    {
    }

    @Override
    public void close()
    {
        connectionPool.close();
    }
}
