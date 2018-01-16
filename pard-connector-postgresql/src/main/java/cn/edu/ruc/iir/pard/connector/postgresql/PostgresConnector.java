package cn.edu.ruc.iir.pard.connector.postgresql;

import cn.edu.ruc.iir.pard.catalog.Column;
import cn.edu.ruc.iir.pard.commons.config.PardUserConfiguration;
import cn.edu.ruc.iir.pard.commons.memory.Row;
import cn.edu.ruc.iir.pard.commons.utils.DataType;
import cn.edu.ruc.iir.pard.commons.utils.RowConstructor;
import cn.edu.ruc.iir.pard.executor.connector.Connector;
import cn.edu.ruc.iir.pard.executor.connector.CreateSchemaTask;
import cn.edu.ruc.iir.pard.executor.connector.CreateTableTask;
import cn.edu.ruc.iir.pard.executor.connector.CreateTmpTableTask;
import cn.edu.ruc.iir.pard.executor.connector.DeleteTask;
import cn.edu.ruc.iir.pard.executor.connector.DropSchemaTask;
import cn.edu.ruc.iir.pard.executor.connector.DropTableTask;
import cn.edu.ruc.iir.pard.executor.connector.InsertIntoTask;
import cn.edu.ruc.iir.pard.executor.connector.JoinTask;
import cn.edu.ruc.iir.pard.executor.connector.LoadTask;
import cn.edu.ruc.iir.pard.executor.connector.PardResultSet;
import cn.edu.ruc.iir.pard.executor.connector.QueryTask;
import cn.edu.ruc.iir.pard.executor.connector.SendDataTask;
import cn.edu.ruc.iir.pard.executor.connector.Task;
import cn.edu.ruc.iir.pard.executor.connector.node.FilterNode;
import cn.edu.ruc.iir.pard.executor.connector.node.JoinNode;
import cn.edu.ruc.iir.pard.executor.connector.node.LimitNode;
import cn.edu.ruc.iir.pard.executor.connector.node.PlanNode;
import cn.edu.ruc.iir.pard.executor.connector.node.ProjectNode;
import cn.edu.ruc.iir.pard.executor.connector.node.SortNode;
import cn.edu.ruc.iir.pard.executor.connector.node.TableScanNode;
import cn.edu.ruc.iir.pard.sql.expr.ColumnItem;
import cn.edu.ruc.iir.pard.sql.expr.Expr;
import cn.edu.ruc.iir.pard.sql.expr.FalseExpr;
import cn.edu.ruc.iir.pard.sql.expr.TrueExpr;
import cn.edu.ruc.iir.pard.sql.expr.ValueItem;
//import cn.edu.ruc.iir.pard.sql.tree.ComparisonExpression;
import cn.edu.ruc.iir.pard.sql.tree.Expression;
//import cn.edu.ruc.iir.pard.sql.tree.Table;
import com.google.common.collect.ImmutableList;
import org.postgresql.copy.CopyManager;
import org.postgresql.jdbc.PgConnection;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
                return executeBatchInsertInto(conn, (InsertIntoTask) task);
            }
            if (task instanceof LoadTask) {
                return executeLoad(conn, (LoadTask) task);
            }
            if (task instanceof DeleteTask) {
                return executeDelete(conn, (DeleteTask) task);
            }
            if (task instanceof JoinTask) {
                return executeJoin(conn, (JoinTask) task);
            }
            if (task instanceof SendDataTask) {
                return executeSendDataTask(conn, (SendDataTask) task);
            }
            if (task instanceof CreateTmpTableTask) {
                return executeCreateTmpTable(conn, (CreateTmpTableTask) task);
            }
            if (task instanceof JoinTask) {
                return executeJoin(conn, (JoinTask) task);
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
            PardResultSet prs = new PardResultSet(PardResultSet.ResultStatus.OK, columns);
            prs.setJdbcResultSet(rs);
            prs.setJdbcConnection(conn);
            return prs;
        }
        catch (SQLException e) {
            logger.info("QUERY FAILED");
            e.printStackTrace();
        }
        return PardResultSet.execErrResultSet;
    }

    private PardResultSet executeLoad(Connection conn, LoadTask task)
    {
        String schema = task.getSchema();
        String table = task.getTable();
        List<String> paths = task.getPaths();
        try {
            PgConnection pgConnection;
            if (!conn.isWrapperFor(PgConnection.class)) {
                return PardResultSet.execErrResultSet;
            }
            pgConnection = conn.unwrap(PgConnection.class);
            CopyManager copyManager = new CopyManager(pgConnection);
            for (String path : paths) {
                logger.info("Copying " + path + " into " + schema + "." + table);
                String sql = "COPY " + schema + "." + table + " FROM STDIN DELIMITER E'\t'";
                logger.info("Postgres connector: " + sql);
                File file = new File(path);
                InputStream inputStream = new FileInputStream(file);
                copyManager.copyIn(sql, inputStream);
                file.deleteOnExit();
            }
            PardResultSet resultSet = new PardResultSet(PardResultSet.ResultStatus.OK);
            RowConstructor rowConstructor = new RowConstructor();
            rowConstructor.appendString(PardResultSet.ResultStatus.OK.toString());
            resultSet.add(rowConstructor.build());
            return resultSet;
        }
        catch (SQLException | IOException e) {
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

    private PardResultSet executeDelete(Connection conn, DeleteTask task)
    {
        String schema = task.getSchema();
        String table = task.getTable();
        try {
            Statement statement = conn.createStatement();
            StringBuilder sb = new StringBuilder("DELETE FROM ");
            sb.append(schema).append(".").append(table);
            sb.append(" WHERE ");
            sb.append(task.getExpression().toString());
            logger.info("Postgres connector: " + sb.toString());
            statement.executeUpdate(sb.toString());
            return PardResultSet.okResultSet;
        }
        catch (SQLException e) {
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

    private PardResultSet executeSendDataTask(Connection conn, SendDataTask task)
    {
        String schema = task.getSchemaName();
        String table = null;
        try {
            Statement statement = conn.createStatement();
            StringBuilder querySQL = new StringBuilder("select ");
            PlanNode rootNode = task.getNode();
            List<PlanNode> nodeList = new ArrayList<>();
            int nodeListCursor = 0;
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
                    table = ((TableScanNode) nodeList.get(i)).getTable();
                    schema = ((TableScanNode) nodeList.get(i)).getSchema();
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
//            if (isProject) {
            List<Column> cols = projectNode.getColumns();
            for (Column column : cols) {
                querySQL.append(column.getColumnName());
                querySQL.append(",");
            }
            querySQL = new StringBuilder(querySQL.substring(0, querySQL.length() - 1));
//            }
//            else {
//                querySQL.append(" *");
//            }
            querySQL.append(" from ");
            querySQL.append(schema);
            querySQL.append(".");
            querySQL.append(table);
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

            Map<String, Expression> siteExpression = task.getSiteExpression(); // site -> Expression
            Map<String, String> tmpTableMap = task.getTmpTableMap(); // site -> tmpTableName

            boolean flag = dispense(siteExpression, tmpTableMap, rs, cols, schema, table);
            if (flag == true) {
                conn.close();
                return PardResultSet.okResultSet;
            }
            else {
                conn.close();
                return PardResultSet.execErrResultSet;
            }
        }
        catch (SQLException e) {
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

    public PardResultSet executeCreateTmpTable(Connection conn, CreateTmpTableTask task)
    {
        String schemaName = task.getSchemaName();
        String tableName = task.getTableName();
        DropTableTask dropTableTask = new DropTableTask(schemaName, tableName);
        PardResultSet prsDropTempTale = dropTempTable(conn, dropTableTask);
        CreateTableTask createTableTask = new CreateTableTask(schemaName, tableName, false, task.getColumnDefinitions());
        PardResultSet prsCreateTempTable = createTempTable(conn, createTableTask);
        String filePath1 = task.getPath();
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(filePath1)));
            BufferedWriter bw = new BufferedWriter(new FileWriter(new File(filePath1 + "tmp")));
            String readIn = br.readLine();
            readIn = br.readLine();
            while ((readIn = br.readLine()) != null) {
                bw.write(readIn);
                bw.newLine();
            }
            br.close();
            bw.flush();
            bw.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        LoadTask loadTask = new LoadTask(schemaName, tableName, ImmutableList.of(filePath1 + "tmp"));
        PardResultSet prsLoadTemoTable = loadTmpTable(conn, loadTask);
        return prsLoadTemoTable;
    }

    private PardResultSet loadTmpTable(Connection conn, LoadTask task)
    {
        String schema = task.getSchema();
        String table = task.getTable();
        List<String> paths = task.getPaths();
        try {
            PgConnection pgConnection;
            if (!conn.isWrapperFor(PgConnection.class)) {
                return PardResultSet.execErrResultSet;
            }
            pgConnection = conn.unwrap(PgConnection.class);
            CopyManager copyManager = new CopyManager(pgConnection);
            for (String path : paths) {
                if (schema != null) {
                    logger.info("Copying " + path + " into " + schema + "." + table);
                    String sql = "COPY " + schema + "." + table + " FROM STDIN DELIMITER E'\t'";
                    logger.info("Postgres connector: " + sql);
                    File file = new File(path);
                    InputStream inputStream = new FileInputStream(file);
                    copyManager.copyIn(sql, inputStream);
                    file.deleteOnExit();
                }
                else {
                    logger.info("Copying " + path + " into " + table);
                    String sql = "COPY " + table + " FROM STDIN DELIMITER E'\t'";
                    logger.info("Postgres connector: " + sql);
                    File file = new File(path);
                    InputStream inputStream = new FileInputStream(file);
                    copyManager.copyIn(sql, inputStream);
                    file.deleteOnExit();
                }
            }
            PardResultSet resultSet = new PardResultSet(PardResultSet.ResultStatus.OK);
            RowConstructor rowConstructor = new RowConstructor();
            rowConstructor.appendString(PardResultSet.ResultStatus.OK.toString());
            resultSet.add(rowConstructor.build());
            return resultSet;
        }
        catch (SQLException | IOException e) {
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

    private PardResultSet dropTempTable(Connection conn, DropTableTask task)
    {
        try {
            Statement statement = conn.createStatement();
            String dropTableSQL;
            if (task.getSchemaName() == null) {
                dropTableSQL = "drop table if exists " + task.getTableName();
            }
            else {
                dropTableSQL = "drop table if exists " + task.getSchemaName() + "." + task.getTableName();
            }
            logger.info("Postgres connector: " + dropTableSQL);
            int status = statement.executeUpdate(dropTableSQL);
            if (status == 0) {
                logger.info("DROP TEMP TABLE SUCCESSFULLY");
                return PardResultSet.okResultSet;
            }
        }
        catch (SQLException e) {
            logger.info("DROP TEMP TABLE FAILED");
            e.printStackTrace();
        }
        return PardResultSet.execErrResultSet;
    }

    private PardResultSet createTempTable(Connection conn, CreateTableTask task)
    {
        try {
            StringBuilder createTableSQL = new StringBuilder("create table if not exists ");
            if (task.getSchemaName() != null) {
                createTableSQL.append(task.getSchemaName() + "." + task.getTableName() + "(");
            }
            else {
                createTableSQL.append(task.getTableName() + "(");
            }
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
                logger.info("CREATE TEMP TABLE SUCCESSFULLY");
                return PardResultSet.okResultSet;
            }
        }
        catch (SQLException e) {
            logger.info("CREATE TEMP TABLE FAILED");
            e.printStackTrace();
        }
        return PardResultSet.execErrResultSet;
    }

    private PardResultSet executeJoin(Connection conn, JoinTask task)
    {
        String tmpTableName = task.getTmpTableName();
        PlanNode rootNode = task.getNode();
        try {
            Statement statement = conn.createStatement();
            StringBuilder joinSQL = new StringBuilder("select ");
            List<PlanNode> nodeList = new ArrayList<>();
            int nodeListCursor = 0;
            boolean isProject = false;
            boolean isSort = false;
            boolean isLimit = false;
            ProjectNode projectNode = null;
            LimitNode limitNode = null;
            SortNode sortNode = null;
            JoinNode joinNode = null;
            nodeList.add(rootNode);
            nodeListCursor++;

            while (nodeList.get(nodeListCursor - 1).hasChildren()) {
                nodeList.add(nodeList.get(nodeListCursor - 1).getLeftChild());
                nodeListCursor++;
                if (nodeList.get(nodeListCursor - 1) instanceof JoinNode) {
                    joinNode = (JoinNode) nodeList.get(nodeListCursor - 1);
                    break;
                }
            }

            for (int i = nodeListCursor - 1; i >= 0; i--) {
                if (nodeList.get(i) instanceof LimitNode) {
                    limitNode = (LimitNode) nodeList.get(i);
                    isLimit = true;
                }

                if (nodeList.get(i) instanceof SortNode) {
                    sortNode = (SortNode) nodeList.get(i);
                    isSort = true;
                }

                if (nodeList.get(i) instanceof ProjectNode) {
                    projectNode = (ProjectNode) nodeList.get(i);
                    isProject = true;
                }
            }

            if (isProject) {
                List<Column> columns = projectNode.getColumns();
                for (Column column : columns) {
                    joinSQL.append(column.getTableName() + "." + column.getColumnName());
                    joinSQL.append(",");
                }
                joinSQL = new StringBuilder(joinSQL.substring(0, joinSQL.length() - 1));
            }
            else {
                joinSQL.append(" *");
            }

            StringBuilder fromClause = new StringBuilder(" from ");
            StringBuilder whereClause = new StringBuilder(" where ");
            List<PlanNode> joinChildren = joinNode.getJoinChildren();
            Iterator it = joinChildren.iterator();
            Boolean isFirst = true;
            while (it.hasNext()) {
                PlanNode childRootNode = (PlanNode) it.next();
                List<PlanNode> childNodeList = new ArrayList<>();
                int childNodeListCursor = 0;
                ProjectNode childProjectNode = null;
                FilterNode childFilterNode = null;
                TableScanNode childTableScanNode = null;
                boolean childIsProject = false;
                boolean childIsFilter = false;
                boolean childIsTableScan = false;
                childNodeList.add(childRootNode);
                childNodeListCursor++;
                while (childNodeList.get(childNodeListCursor - 1).hasChildren()) {
                    childNodeList.add(childNodeList.get(childNodeListCursor - 1).getLeftChild());
                    childNodeListCursor++;
                }
                for (int i = childNodeListCursor - 1; i >= 0; i--) {
                    if (childNodeList.get(i) instanceof ProjectNode) {
                        childProjectNode = (ProjectNode) childNodeList.get(i);
                        childIsProject = true;
                    }
                    if (childNodeList.get(i) instanceof FilterNode) {
                        childFilterNode = (FilterNode) childNodeList.get(i);
                        childIsFilter = true;
                    }
                    if (childNodeList.get(i) instanceof TableScanNode) {
                        childTableScanNode = (TableScanNode) childNodeList.get(i);
                        childIsTableScan = true;
                    }
                }
                //HERE WE IGNORE THE childProjectNode
                if (childIsFilter) {
                    whereClause.append("and " + childFilterNode.getExpression());
                }
                if (childIsTableScan) {
                    String schemaName = childTableScanNode.getSchema();
                    String tableName = childTableScanNode.getTable();
                    String aliasName = childTableScanNode.getAlias();
                    if (isFirst) {
                        fromClause.append(schemaName + "." + tableName);
                        fromClause.append("inner join ");
                        isFirst = false;
                    }
                    else {
                        fromClause.append(schemaName + "." + aliasName);
                        fromClause.append(" on ");
                    }
                }
            }
            if (joinNode.getExprList().size() > 0) {
                whereClause.append(joinNode.getExprList().get(0).toString());
            }
            else {
                whereClause.append((String) (joinNode.getJoinSet().iterator().next()));
            }
            if (isSort) {
                whereClause.append("order by");
                List<Column> columns = sortNode.getColumns();
                for (Column column : columns) {
                    whereClause.append(" ");
                    whereClause.append(column.getTableName() + "." + column.getColumnName());
                    whereClause.append(",");
                }
                whereClause = new StringBuilder(whereClause.substring(0, whereClause.length() - 1));
            }
            if (isLimit) {
                whereClause.append(" limit ");
                whereClause.append(limitNode.getLimitNum());
            }
            joinSQL.append(fromClause.toString() + whereClause.toString());
            logger.info("Postgres connector: " + joinSQL);
            ResultSet rs = statement.executeQuery(joinSQL.toString());
            List<Column> columns = new ArrayList<>();
            if (isProject) {
                columns = projectNode.getColumns();
            }
            logger.info("JOIN SUCCESSFULLY");
            PardResultSet prs = new PardResultSet(PardResultSet.ResultStatus.OK, columns);
            prs.setJdbcResultSet(rs);
            prs.setJdbcConnection(conn);
            return prs;
        }
        catch (SQLException e) {
            logger.info("JOIN FAILED");
            e.printStackTrace();
        }
        return PardResultSet.execErrResultSet;
    }

    private boolean dispense(Map<String, Expression> siteExpression, Map<String, String> tmpTableMap, ResultSet rs, List<Column> columns, String schema, String table)
    {
        boolean isSucceeded;
        Map<String, BufferedWriter> localWriter = new HashMap<String, BufferedWriter>(); // site -> local BufferedWirter
        for (String site : siteExpression.keySet()) {
            try {
                BufferedWriter bw = new BufferedWriter(new FileWriter(new File("/dev/shm/" + site + tmpTableMap.get(site) + "SENDDATA")));
                localWriter.put(site, bw);
                bw.write(schema + "\t" + tmpTableMap.get(site) + "\t" + table + "\n"); //schema name, table name
                Iterator it = columns.iterator();
                String secondLine = "";
                while (it.hasNext()) {
                    secondLine += ((Column) it.next()).getColumnName() + "\t";   // column names
                }
                secondLine = secondLine.substring(0, secondLine.length() - 1);
                bw.write(secondLine);
                bw.write("\n");
                bw.flush();
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }

        try {
            ResultSetMetaData rsmd = rs.getMetaData();
            int colNum = rsmd.getColumnCount();
            while (rs.next()) {
                RowConstructor rowConstructor = new RowConstructor();
                List<Integer> colTypes = new ArrayList<>();

                for (int i = 0; i < colNum; i++) {
                    switch (rsmd.getColumnType(i + 1)) {
                        case Types.CHAR:
                            rowConstructor.appendString(rs.getString(i + 1));
                            colTypes.add(DataType.CHAR.getType());
                            break;

                        case Types.VARCHAR:
                            rowConstructor.appendString(rs.getString(i + 1));
                            colTypes.add(DataType.VARCHAR.getType());
                            break;

                        case Types.DATE:
                            rowConstructor.appendString(rs.getString(i + 1).toString());
                            colTypes.add(DataType.DATE.getType());
                            break;

                        case Types.INTEGER:
                            rowConstructor.appendInt(rs.getInt(i + 1));
                            colTypes.add(DataType.INT.getType());
                            break;

                        case Types.FLOAT:
                            rowConstructor.appendFloat(rs.getFloat(i + 1));
                            colTypes.add(DataType.FLOAT.getType());
                            break;

                        case Types.DOUBLE:
                            rowConstructor.appendDouble(rs.getDouble(i + 1));
                            colTypes.add(DataType.DOUBLE.getType());
                            break;

                        default:
                            break;
                    }
                }
                Row row = rowConstructor.build();
                for (Map.Entry<String, Expression> entry : siteExpression.entrySet()) {
                    boolean ifTrue = compare(entry.getValue(), row, columns);
                    if (ifTrue) {
                        localWriter.get(entry.getKey()).write(rowConstructor.printRow(row, colTypes) + "\n");
                    }
                }
            }

            for (Map.Entry<String, BufferedWriter> entry : localWriter.entrySet()) {
                entry.getValue().flush();
                entry.getValue().close();
            }

            isSucceeded = true;
            return isSucceeded;
        }
        catch (SQLException e) {
            e.printStackTrace();
            isSucceeded = false;
        }
        catch (IOException e) {
            e.printStackTrace();
            isSucceeded = false;
        }
        return isSucceeded;
    }

    private Boolean compare(Expression expr, Row row, List<Column> col)
    {
        List<Integer> types = new ArrayList<Integer>();
        col.forEach(x -> types.add(x.getDataType()));
        String[] list = RowConstructor.printRow(row, types).split("\t");
        List<ColumnItem> ciList = new ArrayList<ColumnItem>();
        List<ValueItem> vList = new ArrayList<ValueItem>();
        Expr e = Expr.parse(expr);
        for (int i = 0; i < list.length; i++) {
            ColumnItem ci = new ColumnItem(col.get(i).getTableName(), col.get(i).getColumnName(), col.get(i).getDataType());
            ValueItem vi = new ValueItem(parseFromString(col.get(i).getDataType(), list[i]));
            ciList.add(ci);
            vList.add(vi);
        }
        for (int i = 0; i < list.length; i++) {
            ColumnItem ci = ciList.get(i);
            ValueItem vi = vList.get(i);
            e = Expr.generalReplace(e, ci, vi);
        }
//        System.out.println(e.toString());
        e = Expr.optimize(e, Expr.LogicOperator.AND);
        if (e instanceof TrueExpr) {
            return true;
        }
        else if (e instanceof FalseExpr) {
            return false;
        }
        return null;
    }

    private static Comparable parseFromString(int dataType, String value)
    {
        switch(dataType) {
            case DataType.DataTypeInt.SMALLINT:
            case DataType.DataTypeInt.BIGINT:
            case DataType.DataTypeInt.INT:
                return Long.parseLong(value);
            case DataType.DataTypeInt.FLOAT:
            case DataType.DataTypeInt.DOUBLE:
                return Double.parseDouble(value);
            case DataType.DataTypeInt.TEXT:
            case DataType.DataTypeInt.CHAR:
            case DataType.DataTypeInt.VARCHAR:
                return value;
            case DataType.DataTypeInt.TIME:
            case DataType.DataTypeInt.DATE:
            case DataType.DataTypeInt.TIMESTAMP:
                return value;
        }
        return value;
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
