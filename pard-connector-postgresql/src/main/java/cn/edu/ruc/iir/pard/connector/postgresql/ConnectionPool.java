package cn.edu.ruc.iir.pard.connector.postgresql;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * pard
 *
 * @author guodong
 */
public class ConnectionPool
{
    private final HikariDataSource dataSource;

    public ConnectionPool(
            String connectorDriver,
            String connectorHost,
            String connectorUser,
            String connectorPassword)
    {
        HikariConfig config = new HikariConfig();
        /*
        config.setDriverClassName(connectorDriver);
        config.setJdbcUrl(connectorHost);
        config.setUsername(connectorUser);
        config.setPassword(connectorPassword);
        */
        config.setDriverClassName("org.postgresql.ds.PGSimpleDataSource");
        config.setUsername("pard");
        config.setPassword("pard500");
        config.setJdbcUrl("jdbc:postgresql://localhost:5432/parddb");
        config.setMinimumIdle(10); // minimum pool size
        config.setMaximumPoolSize(50); //maximum pool size
        dataSource = new HikariDataSource(config);
    }

    public Connection getConnection() throws SQLException
    {
        return dataSource.getConnection();
    }

    public void close()
    {
        dataSource.close();
    }
}
