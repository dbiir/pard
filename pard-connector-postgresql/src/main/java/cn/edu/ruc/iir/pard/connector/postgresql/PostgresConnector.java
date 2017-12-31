package cn.edu.ruc.iir.pard.connector.postgresql;

import cn.edu.ruc.iir.pard.catalog.Column;
import cn.edu.ruc.iir.pard.catalog.DataType;
import cn.edu.ruc.iir.pard.commons.config.PardUserConfiguration;
import cn.edu.ruc.iir.pard.commons.utils.PardResultSet;
import cn.edu.ruc.iir.pard.executor.connector.*;

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
            if (task instanceof DropSchemaTask) {
                return executeDropSchema(conn, (DropSchemaTask) task);
            }
            if (task instanceof DropTableTask) {
                return executeDropTable(conn, (DropTableTask) task);
            }
            if (task instanceof InsertIntoTask) {
                return executeInsertInto(conn, (InsertIntoTask) task);
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
            createSchemaSQL = "create schema " + task.getSchemaName();
            int status = statement.executeUpdate(createSchemaSQL);
            if (status == 0) {
                System.out.println("CREATE SCHEMA SUCCESSFULLY");
                return new PardResultSet(PardResultSet.ResultStatus.OK);
            }
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
                if (cd.getKey() == 1) {
                    createTableSQL = createTableSQL + cd.getColumnName() + " " + getTypeString(cd.getDataType(), cd.getLen()) + " primary key ";
                }
                else {
                    createTableSQL = createTableSQL + cd.getColumnName() + " " + getTypeString(cd.getDataType(), cd.getLen());
                }
                createTableSQL = createTableSQL + " ,";
            }
            createTableSQL = createTableSQL.substring(0, createTableSQL.length() - 1);
            createTableSQL = createTableSQL + ")";
            System.out.println(createTableSQL);
            Statement statement = conn.createStatement();
            int status = statement.executeUpdate(createTableSQL);
            if (status == 0) {
                System.out.println("CREATE TABLE SUCCESSFULLY");
                return new PardResultSet(PardResultSet.ResultStatus.OK);
            }
        }
        catch (SQLException e) {
            System.out.println("CREATE TABLE FAILED");
            e.printStackTrace();
        }
        return new PardResultSet(PardResultSet.ResultStatus.EXECUTING_ERR);
    }

    public PardResultSet executeDropSchema(Connection conn, DropSchemaTask task)
    {
        try{
            Statement statement = conn.createStatement();
            String dropSchemaSQL;
            dropSchemaSQL = "drop schema " + task.getSchema() + " CASCADE";
            int status = statement.executeUpdate(dropSchemaSQL);
            if (status == 0) {
                System.out.println("DROP SCHEMA SUCCESSFULLY");
                return new PardResultSet(PardResultSet.ResultStatus.OK);
            }
        }
        catch (SQLException e) {
            System.out.println("DROP SCHEMA FAILED");
            e.printStackTrace();
        }
        return new PardResultSet(PardResultSet.ResultStatus.EXECUTING_ERR);
    }

    public PardResultSet executeDropTable (Connection conn, DropTableTask task)
    {
        try{
            Statement statement = conn.createStatement();
            String dropTableSQL;
            if(task.getSchemaName() == null) {
                dropTableSQL = "drop table " + task.getTableName();
            } else {
                dropTableSQL = "drop table " + task.getSchemaName() + "." + task.getTableName();
            }
            int status = statement.executeUpdate(dropTableSQL);
            if (status == 0) {
                System.out.println("DROP TABLE SUCCESSFULLY");
                return new PardResultSet(PardResultSet.ResultStatus.OK);
            }
        }
        catch (SQLException e) {
            System.out.println("DROP TABLE FAILED");
            e.printStackTrace();
        }
        return new PardResultSet(PardResultSet.ResultStatus.EXECUTING_ERR);
    }

    public PardResultSet executeInsertInto(Connection conn, InsertIntoTask task)
    {
        return new PardResultSet(PardResultSet.ResultStatus.OK);
    }

    private String getTypeString(int type, int length)
    {
        if (type == DataType.INT.getType()) {
            return "int";
        }
        if (type == DataType.FLOAT.getType()) {
            return "float";
        }
        if (type == DataType.CHAR.getType()) {
            return "char(" + length + ")";
        }
        // todo add more types
        return null;
    }
}
