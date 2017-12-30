package cn.edu.ruc.iir.pard.connector.postgresql;

import cn.edu.ruc.iir.pard.catalog.Column;
import cn.edu.ruc.iir.pard.commons.config.PardUserConfiguration;
import cn.edu.ruc.iir.pard.commons.utils.PardResultSet;
import cn.edu.ruc.iir.pard.executor.connector.Connector;
import cn.edu.ruc.iir.pard.executor.connector.CreateSchemaTask;
import cn.edu.ruc.iir.pard.executor.connector.CreateTableTask;
import cn.edu.ruc.iir.pard.executor.connector.Task;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;

/**
 * pard
 *
 * @author guodong
 */
public class PostgresConnector
        implements Connector
{
    private final ConnectionPool connectionPool;

    private static final class PostgresConnectorHolder
    {
        private static final PostgresConnector instance = new PostgresConnector();
    }

    public static final PostgresConnector INSTANCE()
    {
        return PostgresConnectorHolder.instance;
    }

    private PostgresConnector()
    {
        PardUserConfiguration configuration = PardUserConfiguration.INSTANCE();

        connectionPool = new ConnectionPool(
                configuration.getConnectorDriver(),
                configuration.getConnectorHost(),
                configuration.getConnectorUser(),
                configuration.getConnectorPassword());
    }

    @Override
    public PardResultSet execute(Task task)
    {
        try {
            Connection conn = connectionPool.getConnection();
            if (task instanceof CreateSchemaTask) {
                return executeCreateSchema(conn, (CreateSchemaTask) task);
            }
            if (task instanceof CreateTableTask) {
                return executeCreateTable(conn, (CreateTableTask) task);
            }
        }
        catch (SQLException e) {
            System.out.println("GET CONNECTION FAILED");
            e.printStackTrace();
        }
        return new PardResultSet(PardResultSet.ResultStatus.EXECUTING_ERR);
    }

    @Override
    public void close()
    {
        connectionPool.close();
    }

    public PardResultSet executeCreateSchema(Connection conn, CreateSchemaTask task)
    {
        try {
            Statement statement = conn.createStatement();
            String createSchemaSQL;
            createSchemaSQL = "create schema if not exists " + task.getSchemaName();
            int status = statement.executeUpdate(createSchemaSQL);
            if (status != 0) {
                return new PardResultSet(PardResultSet.ResultStatus.OK);
            }
            System.out.println("CREATE SCHEMA SUCCESSFULLY");
        }
        catch (SQLException e) {
            System.out.println("CREATE SCHEMA FAILED");
            e.printStackTrace();
        }
        return new PardResultSet(PardResultSet.ResultStatus.EXECUTING_ERR);
    }

    public PardResultSet executeCreateTable(Connection conn, CreateTableTask task)
    {
        try {
            String createTableSQL = "create table if not exists " + task.getSchemaName() + "." + task.getTableName() + "(";
            Iterator<Column> it = task.getColumnDefinitions().iterator();
            while (it.hasNext()) {
                Column cd = it.next();
                //if (cd.getPrimary() == true) {
                //    createTableSQL = createTableSQL + cd.getName().getValue() + " " + cd.getType() + " primary key ";
                //}
                //else {
                createTableSQL = createTableSQL + cd.getColumnName() + " " + cd.getDataType();
                //}
                createTableSQL = createTableSQL + " ,";
            }
            createTableSQL = createTableSQL.substring(0, createTableSQL.length() - 1);
            createTableSQL = createTableSQL + ")";
            Statement statement = conn.createStatement();
            int status = statement.executeUpdate(createTableSQL);
            if (status != 0) {
                return new PardResultSet(PardResultSet.ResultStatus.OK);
            }
            System.out.println("CREATE TABLE SUCCESSFULLY");
        }
        catch (SQLException e) {
            System.out.println("CREATE TABLE FAILED");
            e.printStackTrace();
        }
        return new PardResultSet(PardResultSet.ResultStatus.EXECUTING_ERR);
    }
}
