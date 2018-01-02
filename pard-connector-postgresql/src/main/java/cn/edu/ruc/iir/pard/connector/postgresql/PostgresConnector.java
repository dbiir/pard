package cn.edu.ruc.iir.pard.connector.postgresql;

import cn.edu.ruc.iir.pard.catalog.Column;
import cn.edu.ruc.iir.pard.catalog.DataType;
import cn.edu.ruc.iir.pard.commons.config.PardUserConfiguration;
import cn.edu.ruc.iir.pard.commons.memory.Block;
import cn.edu.ruc.iir.pard.commons.memory.Row;
import cn.edu.ruc.iir.pard.commons.utils.PardResultSet;
import cn.edu.ruc.iir.pard.commons.utils.RowConstructor;
import cn.edu.ruc.iir.pard.executor.connector.Connector;
import cn.edu.ruc.iir.pard.executor.connector.CreateSchemaTask;
import cn.edu.ruc.iir.pard.executor.connector.CreateTableTask;
import cn.edu.ruc.iir.pard.executor.connector.DropSchemaTask;
import cn.edu.ruc.iir.pard.executor.connector.DropTableTask;
import cn.edu.ruc.iir.pard.executor.connector.InsertIntoTask;
import cn.edu.ruc.iir.pard.executor.connector.QueryTask;
import cn.edu.ruc.iir.pard.executor.connector.Task;
import cn.edu.ruc.iir.pard.executor.connector.node.FilterNode;
import cn.edu.ruc.iir.pard.executor.connector.node.LimitNode;
import cn.edu.ruc.iir.pard.executor.connector.node.PlanNode;
import cn.edu.ruc.iir.pard.executor.connector.node.ProjectNode;
import cn.edu.ruc.iir.pard.executor.connector.node.SortNode;
import cn.edu.ruc.iir.pard.executor.connector.node.TableScanNode;
import cn.edu.ruc.iir.pard.sql.tree.ComparisonExpression;
import cn.edu.ruc.iir.pard.sql.tree.DoubleLiteral;
import cn.edu.ruc.iir.pard.sql.tree.Identifier;
import cn.edu.ruc.iir.pard.sql.tree.Literal;
import cn.edu.ruc.iir.pard.sql.tree.LongLiteral;
import cn.edu.ruc.iir.pard.sql.tree.StringLiteral;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Iterator;
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
    private int chNum = 0;

    public void setChNum(int num)
    {
        chNum = num;
    }

    public int getChNum()
    {
        return chNum;
    }
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

    private PardResultSet executeCreateSchema(Connection conn, CreateSchemaTask task)
    {
        try {
            Statement statement = conn.createStatement();
            String createSchemaSQL;
            createSchemaSQL = "create schema " + task.getSchemaName();
            int status = statement.executeUpdate(createSchemaSQL);
            if (status == 0) {
                System.out.println("CREATE SCHEMA SUCCESSFULLY");
                close();
                return new PardResultSet(PardResultSet.ResultStatus.OK);
            }
        }
        catch (SQLException e) {
            System.out.println("CREATE SCHEMA FAILED");
            e.printStackTrace();
        }
        close();
        return new PardResultSet(PardResultSet.ResultStatus.EXECUTING_ERR);
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
            //System.out.println(createTableSQL);
            logger.info("Postgres connector: " + createTableSQL.toString());
            Statement statement = conn.createStatement();
            int status = statement.executeUpdate(createTableSQL.toString());
            if (status == 0) {
                System.out.println("CREATE TABLE SUCCESSFULLY");
                close();
                return new PardResultSet(PardResultSet.ResultStatus.OK);
            }
        }
        catch (SQLException e) {
            System.out.println("CREATE TABLE FAILED");
            e.printStackTrace();
        }
        close();
        return new PardResultSet(PardResultSet.ResultStatus.EXECUTING_ERR);
    }

    private PardResultSet executeDropSchema(Connection conn, DropSchemaTask task)
    {
        try {
            Statement statement = conn.createStatement();
            String dropSchemaSQL;
            dropSchemaSQL = "drop schema " + task.getSchema() + " CASCADE";
            int status = statement.executeUpdate(dropSchemaSQL);
            logger.info("Postgres connector: " + dropSchemaSQL);
            if (status == 0) {
                System.out.println("DROP SCHEMA SUCCESSFULLY");
                close();
                return new PardResultSet(PardResultSet.ResultStatus.OK);
            }
        }
        catch (SQLException e) {
            System.out.println("DROP SCHEMA FAILED");
            e.printStackTrace();
        }
        close();
        return new PardResultSet(PardResultSet.ResultStatus.EXECUTING_ERR);
    }

    public PardResultSet executeDropTable(Connection conn, DropTableTask task)
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
                System.out.println("DROP TABLE SUCCESSFULLY");
                close();
                return new PardResultSet(PardResultSet.ResultStatus.OK);
            }
        }
        catch (SQLException e) {
            System.out.println("DROP TABLE FAILED");
            e.printStackTrace();
        }
        close();
        return new PardResultSet(PardResultSet.ResultStatus.EXECUTING_ERR);
    }

    public PardResultSet executeInsertInto(Connection conn, InsertIntoTask task)
    {
        this.chNum = 0;
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
                statement.executeUpdate(insertSQL.toString());
                num++;
            }
            this.chNum = num;
            System.out.println("INSERT SUCCESSFULLY");
            close();
            return new PardResultSet(PardResultSet.ResultStatus.OK);
        }
        catch (SQLException e) {
            System.out.println("INSERT FAILED");
            e.printStackTrace();
        }
        close();
        return new PardResultSet(PardResultSet.ResultStatus.EXECUTING_ERR);
    }

    private PardResultSet executeBatchInsertInto(Connection conn, InsertIntoTask task)
    {
        this.chNum = 0;
        try {
            List<Column> columns = task.getColumns();
            String[][] values = task.getValues();
            int fieldNum = columns.size();
            int tupleNum = values.length;
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
                        pstmt.setString(j + 1, value[j]);
                    }
                }
                pstmt.addBatch();
            }
            pstmt.executeBatch();
            //conn.commit();
            this.chNum = tupleNum;
            System.out.println("INSERT SUCCESSFULLY");
            close();
            return new PardResultSet(PardResultSet.ResultStatus.OK);
        }
        catch (SQLException e) {
            System.out.println("INSERT FAILED");
            e.printStackTrace();
        }
        close();
        return new PardResultSet(PardResultSet.ResultStatus.EXECUTING_ERR);
    }

    private PardResultSet executeQuery(Connection conn, QueryTask task)
    {
        this.chNum = 0;
        try {
            Statement statement = conn.createStatement();
            StringBuilder querySQL = new StringBuilder("select ");
            PlanNode rootnode = task.getPlanNode();
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
            nodeList.add(rootnode);
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
            ResultSetMetaData rsmd = rs.getMetaData();
            int colNum = rsmd.getColumnCount();
            PardResultSet prs = new PardResultSet(PardResultSet.ResultStatus.EOR);
            Block block;
            List<Column> columns;
            List<String> columnNames = new ArrayList<>();
            List<String> columnTypes = new ArrayList<>();

            if (isProject) {
                columns = projectNode.getColumns();
                Iterator it = columns.iterator();
                while (it.hasNext()) {
                    columnNames.add(((Column) it.next()).getColumnName());
                    columnTypes.add(getTypeInString(((Column) it.next()).getDataType()));
                }
                block = new Block(columnNames, columnTypes, 1024 * 1024);
                //getResult(block, rs, rsmd, colNum);
                getResultByRowConstructor(block, rs, rsmd, colNum);
                prs.addBlock(block);
            }
            else {
                for (int i = 0; i < rsmd.getColumnCount(); i++) {
                    columnNames.add(rsmd.getColumnName(i + 1));
                    switch (rsmd.getColumnType(i + 1)) {
                        case Types.CHAR:
                            columnTypes.add("char");
                            break;
                        case Types.VARCHAR:
                            columnTypes.add("varchar");
                            break;
                        case Types.DATE:
                            columnTypes.add("date");
                            break;
                        case Types.INTEGER:
                            columnTypes.add("int");
                            break;
                        case Types.FLOAT:
                            columnTypes.add("float");
                            break;
                        case Types.DOUBLE:
                            columnTypes.add("double");
                            break;
                        default:
                            columnTypes.add("other");
                            break;
                    }
                    //columnTypes.add(getTypeInString(rsmd.getColumnType(i + 1)));
                }
                //System.out.println(columnTypes.get(0));
                //System.out.println(columnTypes.get(1));
                block = new Block(columnNames, columnTypes, 1024 * 1024);
                //getResult(block, rs, rsmd, colNum);
                getResultByRowConstructor(block, rs, rsmd, colNum);
                prs.addBlock(block);
            }
            System.out.println("QUERY SUCCESSFULLY");
            this.chNum = block.getRowSize();
            close();
            return prs;
        }
        catch (SQLException e) {
            System.out.println("QUERY FAILED");
            e.printStackTrace();
        }
        close();
        return new PardResultSet(PardResultSet.ResultStatus.EXECUTING_ERR);
    }

    private void getResult(Block block, ResultSet rs, ResultSetMetaData rsmd, int colNum) throws SQLException
    {
        while (rs.next()) {
            List<byte[]> contents0 = new ArrayList<>();
            List<Integer> offsets0 = new ArrayList<>();
            for (int i = 0; i < colNum; i++) {
                switch (rsmd.getColumnType(i + 1)) {
                    case Types.CHAR: {
                        byte[] tmp = rs.getString(i + 1).getBytes();
                        contents0.add(tmp);
                        offsets0.add(tmp.length);
                    }
                        break;
                    case Types.VARCHAR: {
                        byte[] tmp = rs.getString(i + 1).getBytes();
                        contents0.add(tmp);
                        offsets0.add(tmp.length);
                    }
                        break;
                    case Types.DATE: {
                        byte[] tmp = rs.getDate(i + 1).toString().getBytes();
                        contents0.add(tmp);
                        offsets0.add(tmp.length);
                    }
                        break;
                    case Types.INTEGER: {
                        byte[] tmp = Integer.toString(rs.getInt(i + 1)).getBytes();
                        contents0.add(tmp);
                        offsets0.add(tmp.length);
                    }
                        break;
                    case Types.FLOAT: {
                        byte[] tmp = Float.toString(rs.getFloat(i + 1)).getBytes();
                        contents0.add(tmp);
                        offsets0.add(tmp.length);
                    }
                        break;
                    case Types.DOUBLE: {
                        byte[] tmp = Double.toString(rs.getDouble(i + 1)).getBytes();
                        contents0.add(tmp);
                        offsets0.add(tmp.length);
                    }
                        break;
                    default:
                        break;
                }
            }
            int[]offsets = new int[offsets0.size()];
            int contentsLen = 0;
            for (int i = 0; i < offsets0.size(); i++) {
                offsets[i] = offsets0.get(i);
                contentsLen += offsets[i];
            }
            byte[]contents = new byte[contentsLen];
            int contentsCursor = 0;
            for (int i = 0; i < contents0.size(); i++) {
                for (int j = 0; j < offsets[i]; j++) {
                    contents[contentsCursor] = contents0.get(i)[j];
                    contentsCursor++;
                }
            }
            //assert contentsCursor == contentsLen;
            Row row = new Row(contents, offsets);
            block.addRow(row);
        }
    }

    private void getResultByRowConstructor(Block block, ResultSet rs, ResultSetMetaData rsmd, int colNum) throws SQLException
    {
        while (rs.next()) {
            RowConstructor rowConstructor = new RowConstructor();
            for (int i = 0; i < colNum; i++) {
                switch (rsmd.getColumnType(i + 1)) {
                    case Types.CHAR:
                        rowConstructor.appendString(rs.getString(i + 1));
                        break;
                    case Types.VARCHAR:
                        rowConstructor.appendString(rs.getString(i + 1));
                        break;
                    case Types.DATE:
                        rowConstructor.appendString(rs.getString(i + 1));
                        break;
                    case Types.INTEGER:
                        rowConstructor.appendInt(rs.getInt(i + 1));
                        break;
                    case Types.FLOAT:
                        rowConstructor.appendFloat(rs.getFloat(i + 1));
                        break;
                    case Types.DOUBLE:
                        rowConstructor.appendDouble(rs.getDouble(i + 1));
                        break;
                    default:
                        break;
                }
            }
            block.addRow(rowConstructor.build());
        }
    }

    private String getFilterComparisonExpression(ComparisonExpression expression)
    {
        String sql = "";
        Identifier col = (Identifier) expression.getLeft();
        sql = sql + col.getValue();
        switch (expression.getType()) {
            case EQUAL:
                sql += " = ";
                break;
            case LESS_THAN:
                sql += " < ";
                break;
            case GREATER_THAN:
                sql += " > ";
                break;
            case LESS_THAN_OR_EQUAL:
                sql += " <= ";
                break;
            case GREATER_THAN_OR_EQUAL:
                sql += " >= ";
                break;
            case NOT_EQUAL:
                sql += "!= ";
                break;
            default:
                break;
        }
        Literal lit = (Literal) expression.getRight();
        if (lit instanceof LongLiteral) {
            sql += ((LongLiteral) lit).getValue();
        }
        if (lit instanceof DoubleLiteral) {
            sql += ((DoubleLiteral) lit).getValue();
        }
        if (lit instanceof StringLiteral) {
            sql += "'" + ((StringLiteral) lit).getValue() + "'";
        }
        sql += " ";
        return sql;
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

    private String getTypeInString(int type)
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
            return "char";
        }
        if (type == DataType.VARCHAR.getType()) {
            return "varchar";
        }
        // todo add more types
        return null;
    }
}
