package cn.edu.ruc.iir.pard.connector.postgresql;

import cn.edu.ruc.iir.pard.catalog.Column;
import cn.edu.ruc.iir.pard.commons.config.PardUserConfiguration;
import cn.edu.ruc.iir.pard.commons.utils.DataType;
import cn.edu.ruc.iir.pard.executor.connector.Connector;
import cn.edu.ruc.iir.pard.executor.connector.CreateSchemaTask;
import cn.edu.ruc.iir.pard.executor.connector.CreateTableTask;
import cn.edu.ruc.iir.pard.executor.connector.DropSchemaTask;
import cn.edu.ruc.iir.pard.executor.connector.DropTableTask;
import cn.edu.ruc.iir.pard.executor.connector.InsertIntoTask;
import cn.edu.ruc.iir.pard.executor.connector.PardResultSet;
import cn.edu.ruc.iir.pard.executor.connector.QueryTask;
import cn.edu.ruc.iir.pard.executor.connector.Task;
import cn.edu.ruc.iir.pard.executor.connector.node.FilterNode;
import cn.edu.ruc.iir.pard.executor.connector.node.LimitNode;
import cn.edu.ruc.iir.pard.executor.connector.node.PlanNode;
import cn.edu.ruc.iir.pard.executor.connector.node.ProjectNode;
import cn.edu.ruc.iir.pard.executor.connector.node.SortNode;
import cn.edu.ruc.iir.pard.executor.connector.node.TableScanNode;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 * pard
 *
 * @author guodong
 */
public class PostgresConnector
        implements Connector
{
    private final Logger logger = Logger.getLogger(PostgresConnector.class.getName());
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
            if (task instanceof QueryTask) {
                return executeQuery(conn, (QueryTask) task);
            }
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
                //return executeInsertInto(conn, (InsertIntoTask) task);
                return executeBatchInsertInto(conn, (InsertIntoTask) task);
            }
        }
        catch (SQLException e) {
            logger.info("GET CONNECTION FAILED");
            e.printStackTrace();
        }
        return new PardResultSet(PardResultSet.ResultStatus.EXECUTING_ERR);
    }

    @Override
    public void close()
    {
        logger.info("Connector close");
        connectionPool.close();
    }

    private PardResultSet executeCreateSchema(Connection conn, CreateSchemaTask task)
    {
        try {
            Statement statement = conn.createStatement();
            String createSchemaSQL;
            createSchemaSQL = "create schema " + task.getSchemaName();
            int status = statement.executeUpdate(createSchemaSQL);
            logger.info("Connector: " + createSchemaSQL);
            if (status == 0) {
                logger.info("CREATE SCHEMA SUCCESSFULLY");
                conn.close();
                return PardResultSet.okResultSet;
            }
        }
        catch (SQLException e) {
            logger.info("CREATE SCHEMA FAILED");
            e.printStackTrace();
        }
        finally {
            try {
                conn.close();
            }
            catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return PardResultSet.execErrResultSet;
    }

    private PardResultSet executeCreateTable(Connection conn, CreateTableTask task)
    {
        try {
            StringBuilder createTableSQL = new StringBuilder("create table if not exists " + task.getSchemaName() + "." + task.getTableName() + "(");
            for (Column cd : task.getColumnDefinitions()) {
                if (cd.getKey() == 1) {
                    createTableSQL.append(cd.getColumnName()).append(" ").append(getTypeString(cd.getDataType(), cd.getLen())).append(" primary key ");
                }
                else {
                    createTableSQL.append(cd.getColumnName()).append(" ").append(getTypeString(cd.getDataType(), cd.getLen()));
                }
                createTableSQL.append(" ,");
            }
            createTableSQL = new StringBuilder(createTableSQL.substring(0, createTableSQL.length() - 1));
            createTableSQL.append(")");
            logger.info("Connector: " + createTableSQL.toString());
            Statement statement = conn.createStatement();
            int status = statement.executeUpdate(createTableSQL.toString());
            if (status == 0) {
                logger.info("CREATE TABLE SUCCESSFULLY");
                conn.close();
                return PardResultSet.okResultSet;
            }
        }
        catch (SQLException e) {
            logger.info("CREATE TABLE FAILED");
            e.printStackTrace();
        }
        finally {
            try {
                conn.close();
            }
            catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return PardResultSet.execErrResultSet;
    }

    private PardResultSet executeDropSchema(Connection conn, DropSchemaTask task)
    {
        try {
            Statement statement = conn.createStatement();
            String dropSchemaSQL;
            dropSchemaSQL = "drop schema " + task.getSchema() + " CASCADE";
            int status = statement.executeUpdate(dropSchemaSQL);
            logger.info("Connector: " + dropSchemaSQL);
            if (status == 0) {
                logger.info("DROP SCHEMA SUCCESSFULLY");
                conn.close();
                return PardResultSet.okResultSet;
            }
        }
        catch (SQLException e) {
            logger.info("DROP SCHEMA FAILED");
            e.printStackTrace();
        }
        finally {
            try {
                conn.close();
            }
            catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return PardResultSet.execErrResultSet;
    }

    private PardResultSet executeDropTable(Connection conn, DropTableTask task)
    {
        try {
            Statement statement = conn.createStatement();
            String dropTableSQL;
            if (task.getSchemaName() == null) {
                dropTableSQL = "drop table " + task.getTableName();
            }
            else {
                dropTableSQL = "drop table " + task.getSchemaName() + "." + task.getTableName();
            }
            logger.info("Postgres connector: " + dropTableSQL);
            int status = statement.executeUpdate(dropTableSQL);
            if (status == 0) {
                logger.info("DROP TABLE SUCCESSFULLY");
                conn.close();
                return PardResultSet.okResultSet;
            }
        }
        catch (SQLException e) {
            logger.info("DROP TABLE FAILED");
            e.printStackTrace();
        }
        finally {
            try {
                conn.close();
            }
            catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return PardResultSet.execErrResultSet;
    }

    public PardResultSet executeInsertInto(Connection conn, InsertIntoTask task)
    {
        try {
            Statement statement = conn.createStatement();
            List<Column> columns = task.getColumns();
            String[][] values = task.getValues();
            int fieldNum = columns.size();
            StringBuilder insertSQL;
            int num = 0;
            for (String[] value : values) {
                insertSQL = new StringBuilder(" insert into " + task.getSchemaName() + "." + task.getTableName() + " values(");
                for (int j = 0; j < fieldNum; j++) {
                    int type = columns.get(j).getDataType();
                    if (type == DataType.CHAR.getType() || type == DataType.VARCHAR.getType()) {
                        insertSQL.append("'").append(value[j]).append("'");
                    }
                    else {
                        insertSQL.append(value[j]);
                    }
                    insertSQL.append(",");
                }
                insertSQL = new StringBuilder(insertSQL.substring(0, insertSQL.length() - 1));
                insertSQL.append(")");
                logger.info("Connector: " + insertSQL.toString());
                statement.executeUpdate(insertSQL.toString());
                num++;
            }
            logger.info("INSERT SUCCESSFULLY");
            conn.close();
            return new PardResultSet(PardResultSet.ResultStatus.OK);
        }
        catch (SQLException e) {
            logger.info("INSERT FAILED");
            e.printStackTrace();
        }
        finally {
            try {
                conn.close();
            }
            catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return new PardResultSet(PardResultSet.ResultStatus.EXECUTING_ERR);
    }

    private PardResultSet executeBatchInsertInto(Connection conn, InsertIntoTask task)
    {
        try {
            List<Column> columns = task.getColumns();
            String[][] values = task.getValues();
            int fieldNum = columns.size();
            StringBuilder insertSQL = new StringBuilder(" insert into " + task.getSchemaName() + "." + task.getTableName() + " values(");
            for (int i = 0; i < fieldNum; i++) {
                insertSQL.append("?,");
            }
            insertSQL = new StringBuilder(insertSQL.substring(0, insertSQL.length() - 1));
            insertSQL.append(")");
            PreparedStatement pstmt = conn.prepareStatement(insertSQL.toString());
            for (String[] value : values) {
                for (int j = 0; j < fieldNum; j++) {
                    int type = columns.get(j).getDataType();
                    if (type == DataType.INT.getType()) {
                        pstmt.setInt(j + 1, Integer.parseInt(value[j]));
                    }
                    if (type == DataType.FLOAT.getType()) {
                        pstmt.setFloat(j + 1, Float.parseFloat(value[j]));
                    }
                    if (type == DataType.CHAR.getType() || type == DataType.VARCHAR.getType()) {
                        String v = value[j];
                        // todo this is too implicit to replace all ' in string
                        v = v.replaceAll("'", "");
                        pstmt.setString(j + 1, v);
                    }
                }
                pstmt.addBatch();
            }
            pstmt.executeBatch();
            //conn.commit();
            logger.info("INSERT SUCCESSFULLY");
            conn.close();
            return PardResultSet.okResultSet;
        }
        catch (SQLException e) {
            logger.info("INSERT FAILED");
            e.printStackTrace();
        }
        finally {
            try {
                conn.close();
            }
            catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return PardResultSet.execErrResultSet;
    }

    private PardResultSet executeQuery(Connection conn, QueryTask task)
    {
        try {
            Statement statement = conn.createStatement();
            StringBuilder querySQL = new StringBuilder("select ");
            PlanNode rootNode = task.getPlanNode();
            List<PlanNode> nodeList = new ArrayList<>();
            int nodeListCursor = 0;
            String schemaName = null;
            String tableName = null;
            FilterNode filterNode = null;
            ProjectNode projectNode = null;
            SortNode sortNode = null;
            LimitNode limitNode = null;
            boolean isFilter = false;
            boolean isProject = false;
            boolean isSort = false;
            boolean isLimit = false;
            nodeList.add(rootNode);
            nodeListCursor++;
            while (nodeList.get(nodeListCursor - 1).hasChildren()) {
                nodeList.add(nodeList.get(nodeListCursor - 1).getLeftChild());
                nodeListCursor++;
            }

            for (int i = nodeListCursor - 1; i >= 0; i--) {
                if (nodeList.get(i) instanceof TableScanNode) {
                    tableName = ((TableScanNode) nodeList.get(i)).getTable();
                    schemaName = ((TableScanNode) nodeList.get(i)).getSchema();
                    continue;
                }
                if (nodeList.get(i) instanceof FilterNode) {
                    filterNode = (FilterNode) nodeList.get(i);
                    isFilter = true;
                    continue;
                }
                if (nodeList.get(i) instanceof ProjectNode) {
                    projectNode = (ProjectNode) nodeList.get(i);
                    isProject = true;
                    continue;
                }
                if (nodeList.get(i) instanceof SortNode) {
                    sortNode = (SortNode) nodeList.get(i);
                    isSort = true;
                    continue;
                }
                if (nodeList.get(i) instanceof LimitNode) {
                    limitNode = (LimitNode) nodeList.get(i);
                    isLimit = true;
                }
            }

            if (isProject) {
                List<Column> columns = projectNode.getColumns();
                for (Column column : columns) {
                    querySQL.append(column.getColumnName());
                    querySQL.append(",");
                }
                querySQL = new StringBuilder(querySQL.substring(0, querySQL.length() - 1));
            }
            else {
                querySQL.append(" *");
            }
            querySQL.append(" from ");
            querySQL.append(schemaName);
            querySQL.append(".");
            querySQL.append(tableName);
            if (isFilter) {
                querySQL.append(" where ").append(filterNode.getExpression()).append(" ");
            }
            if (isSort) {
                querySQL.append("order by");
                List<Column> columns = sortNode.getColumns();
                for (Column column : columns) {
                    querySQL.append(" ");
                    querySQL.append(column.getColumnName());
                    querySQL.append(",");
                }
                querySQL = new StringBuilder(querySQL.substring(0, querySQL.length() - 1));
            }
            if (isLimit) {
                querySQL.append(" limit ");
                querySQL.append(limitNode.getLimitNum());
            }
            logger.info("Postgres connector: " + querySQL);

            ResultSet rs = statement.executeQuery(querySQL.toString());
            List<Column> columns = new ArrayList<>();

            if (isProject) {
                columns = projectNode.getColumns();
            }
            logger.info("QUERY SUCCESSFULLY");
            conn.close();
            PardResultSet prs = new PardResultSet(PardResultSet.ResultStatus.OK, columns);
            prs.setJdbcResultSet(rs);
            return prs;
        }
        catch (SQLException e) {
            logger.info("QUERY FAILED");
            e.printStackTrace();
        }
        finally {
            try {
                conn.close();
            }
            catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return PardResultSet.execErrResultSet;
    }

    private String getTypeString(int type, int length)
    {
        if (type == DataType.INT.getType()) {
            return "int";
        }
        if (type == DataType.FLOAT.getType()) {
            return "float";
        }
        if (type == DataType.DOUBLE.getType()) {
            return "double";
        }
        if (type == DataType.DATE.getType()) {
            return "date";
        }
        if (type == DataType.CHAR.getType()) {
            return "char(" + length + ")";
        }
        if (type == DataType.VARCHAR.getType()) {
            return "varchar(" + length + ")";
        }
        // todo add more types
        return null;
    }
}
