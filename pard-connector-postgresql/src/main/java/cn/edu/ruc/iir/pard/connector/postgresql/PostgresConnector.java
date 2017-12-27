package cn.edu.ruc.iir.pard.connector.postgresql;

import cn.edu.ruc.iir.pard.commons.config.PardUserConfiguration;
import cn.edu.ruc.iir.pard.executor.connector.Connector;
import cn.edu.ruc.iir.pard.scheduler.CreateSchemaTask;
import cn.edu.ruc.iir.pard.scheduler.CreateTableTask;
import cn.edu.ruc.iir.pard.scheduler.Task;
import cn.edu.ruc.iir.pard.sql.tree.ColumnDefinition;

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
        try {
            Connection conn = connectionPool.getConnection();
            if (task instanceof CreateSchemaTask) {
                executeCreateSchema(conn, (CreateSchemaTask) task);
            }
            if (task instanceof CreateTableTask) {
                executeCreateTable(conn, (CreateTableTask) task);
            }
        }
        catch (SQLException e) {
            System.out.println("GET CONNECTION FAILED");
            e.printStackTrace();
        }
    }

    @Override
    public void close()
    {
        connectionPool.close();
    }

    public void executeCreateSchema(Connection conn, CreateSchemaTask task)
    {
        try {
            Statement statement = conn.createStatement();
            String createSchemaSQL;
            createSchemaSQL = "create schema if not exists " + task.getSchemaName();
            statement.executeUpdate(createSchemaSQL);
            System.out.println("CREATE SCHEMA SUCCESSFULLY");
        }
        catch (SQLException e) {
            System.out.println("CREATE SCHEMA FAILED");
            e.printStackTrace();
        }
    }

    public void executeCreateTable(Connection conn, CreateTableTask task)
    {
        try {
            int size = task.getColumnDefinitions().size();
            String createTableSQL = "create table if not exists " + task.getSchemaName() + "." + task.getTableName() + "(";
            Iterator<ColumnDefinition> it = task.getColumnDefinitions().iterator();
            while (it.hasNext()) {
                ColumnDefinition cd = it.next();
                //if (cd.getPrimary() == true) {
                //    createTableSQL = createTableSQL + cd.getName().getValue() + " " + cd.getType() + " primary key ";
                //}
                //else {
                createTableSQL = createTableSQL + cd.getName().getValue() + " " + cd.getType();
                //}
                createTableSQL = createTableSQL + " ,";
            }
            createTableSQL = createTableSQL.substring(0, createTableSQL.length() - 1);
            createTableSQL = createTableSQL + ")";
            Statement statement = conn.createStatement();
            statement.executeUpdate(createTableSQL);
            System.out.println("CREATE TABLE SUCCESSFULLY");
        }
        catch (SQLException e) {
            System.out.println("CREATE TABLE FAILED");
            e.printStackTrace();
        }
    }
}
