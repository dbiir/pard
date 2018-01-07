package cn.edu.ruc.iir.pard.executor.connector;

import cn.edu.ruc.iir.pard.catalog.Column;
import cn.edu.ruc.iir.pard.commons.memory.Block;
import cn.edu.ruc.iir.pard.commons.memory.Row;
import cn.edu.ruc.iir.pard.commons.utils.DataType;
import cn.edu.ruc.iir.pard.commons.utils.RowConstructor;
import com.google.common.collect.ImmutableList;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * pard
 *
 * @author guodong
 */
public class PardResultSet
        implements Serializable
{
    private static final long serialVersionUID = 8184501795566412803L;

    public static final PardResultSet okResultSet = new PardResultSet(ResultStatus.OK);
    public static final PardResultSet execErrResultSet = new PardResultSet(ResultStatus.EXECUTING_ERR);
    public static final PardResultSet eorResultSet = new PardResultSet(ResultStatus.EOR);

    public enum ResultStatus
    {
        OK("OK"),
        BEGIN_ERR("Create job error"),
        PARSING_ERR("Parse error"),
        PLANNING_ERR("Plan error"),
        SCHEDULING_ERR("Schedule error"),
        EXECUTING_ERR("Execution error"),
        EOR("End of result set");

        private String msg;

        ResultStatus(String msg)
        {
            this.msg = msg;
        }

        @Override
        public String toString()
        {
            return this.msg;
        }
    }

    private static final int defaultCapacity = 10 * 1024 * 1024;

    private final List<Row> currentRows;  // as local execution result set
    private final List<Column> schema;
    private ResultStatus resultStatus;
    private final transient int capacity;
    private transient String taskId;
    private transient int resultSetNum = 0;
    private transient ResultSet jdbcResultSet;
    private transient Connection connection;
    private transient int currentSize = 0;

    public PardResultSet()
    {
        this(ResultStatus.OK, ImmutableList.of(), defaultCapacity);
    }

    public PardResultSet(ResultStatus resultStatus)
    {
        this(resultStatus, ImmutableList.of(), defaultCapacity);
    }

    public PardResultSet(ResultStatus resultStatus, List<Column> schema)
    {
        this(resultStatus, schema, defaultCapacity);
    }

    public PardResultSet(ResultStatus resultStatus, List<Column> schema, int capacity)
    {
        this.capacity = capacity;
        this.resultStatus = resultStatus;
        this.schema = schema;
        this.currentRows = new LinkedList<>();
    }

    /**
     * Merge with another result set.
     * Ignore capacity limit.
     * */
    public void addResultSet(PardResultSet resultSet)
    {
        if (resultSet.getStatus() != ResultStatus.OK && resultSet.getStatus() != ResultStatus.EOR) {
            this.resultStatus = resultSet.resultStatus;
        }
        else {
            this.currentRows.addAll(resultSet.currentRows);
        }
        this.resultSetNum++;
    }

    public void addBlock(Block block)
    {
        currentRows.addAll(block.getRows());
    }

    public List<Row> getRows()
    {
        return currentRows;
    }

    public String getTaskId()
    {
        return taskId;
    }

    public void setTaskId(String taskId)
    {
        this.taskId = taskId;
    }

    public void setJdbcResultSet(ResultSet jdbcResultSet)
    {
        this.jdbcResultSet = jdbcResultSet;
    }

    public void setJdbcConnection(Connection connection)
    {
        this.connection = connection;
    }

    public ResultStatus getStatus()
    {
        return resultStatus;
    }

    public int getResultSetNum()
    {
        return resultSetNum;
    }

    public List<Column> getSchema()
    {
        return this.schema;
    }

    public boolean add(Row row)
    {
        return add(row, true);
    }

    private boolean add(Row row, boolean check)
    {
        if (!check) {
            currentRows.add(row);
            currentSize += row.getSize();
            return true;
        }
        if (currentSize + row.getSize() >= capacity) {
            return false;
        }
        currentRows.add(row);
        currentSize += row.getSize();
        return true;
    }

    /**
     * Get next block in result set.
     * @return null if no block available.
     * */
    public Row getNext()
    {
        if (currentRows.size() == 0) {
            // if current no content, try fetch
            fetch();
        }
        if (currentRows.size() == 0) {
            // no more result, close connection
            try {
                connection.close();
            }
            catch (SQLException e) {
                e.printStackTrace();
            }
            return null;
        }
        // return header row
        Row row = currentRows.remove(0);
        currentSize -= row.getSize();
        return row;
    }

    private void fetch()
    {
        if (jdbcResultSet != null) {
            try {
                while (jdbcResultSet.next()) {
                    RowConstructor rowConstructor = new RowConstructor();
                    for (int i = 0; i < schema.size(); i++) {
                        Column column = schema.get(i);
                        if (column.getDataType() == DataType.CHAR.getType()
                                || column.getDataType() == DataType.VARCHAR.getType()
                                || column.getDataType() == DataType.DATE.getType()) {
                            rowConstructor.appendString(jdbcResultSet.getString(i + 1).trim());
                            continue;
                        }
                        if (column.getDataType() == DataType.INT.getType()
                                || column.getDataType() == DataType.SMALLINT.getType()) {
                            rowConstructor.appendInt(jdbcResultSet.getInt(i + 1));
                            continue;
                        }
                        if (column.getDataType() == DataType.FLOAT.getType()) {
                            rowConstructor.appendFloat(jdbcResultSet.getFloat(i + 1));
                            continue;
                        }
                        if (column.getDataType() == DataType.DOUBLE.getType()) {
                            rowConstructor.appendDouble(jdbcResultSet.getDouble(i + 1));
                            continue;
                        }
                    }
                    Row row = rowConstructor.build();
                    // if result set is full, add this row as the last one and break
                    if (!this.add(row)) {
                        this.add(row, false);
                        break;
                    }
                }
            }
            catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("task", taskId)
                .add("status", resultStatus)
                .omitNullValues()
                .toString();
    }
}
