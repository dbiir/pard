package cn.edu.ruc.iir.pard.executor.connector;

import cn.edu.ruc.iir.pard.catalog.Column;
import cn.edu.ruc.iir.pard.commons.memory.Block;
import cn.edu.ruc.iir.pard.commons.memory.Row;
import cn.edu.ruc.iir.pard.commons.utils.DataType;
import cn.edu.ruc.iir.pard.commons.utils.RowConstructor;
import com.google.common.collect.ImmutableList;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
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

    private static final int defaultCapcity = 10;

    private final List<Block> currentBlocks;
    private final List<Column> schema;
    private final int capacity;
    private ResultStatus resultStatus;
    private String taskId;
    private int resultSeqId;
    private boolean resultHasNext;
    private int resultSetNum = 0;
    private ResultSet jdbcResultSet;
    private Row currentRow;

    public PardResultSet()
    {
        this(ResultStatus.OK, ImmutableList.of(), defaultCapcity);
    }

    public PardResultSet(ResultStatus resultStatus)
    {
        this(resultStatus, ImmutableList.of(), defaultCapcity);
    }

    public PardResultSet(ResultStatus resultStatus, List<Column> schema)
    {
        this(resultStatus, schema, defaultCapcity);
    }

    public PardResultSet(ResultStatus resultStatus, List<Column> schema, int capcity)
    {
        this.capacity = capcity;
        this.resultStatus = resultStatus;
        this.schema = schema;
        currentBlocks = new ArrayList<>(this.capacity);
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
            while (resultSet.hasNext()) {
                currentBlocks.add(resultSet.getNext());
            }
        }
        this.resultSetNum++;
    }

    public String getTaskId()
    {
        return taskId;
    }

    public void setTaskId(String taskId)
    {
        this.taskId = taskId;
    }

    public int getResultSeqId()
    {
        return resultSeqId;
    }

    public void setResultSeqId(int resultSeqId)
    {
        this.resultSeqId = resultSeqId;
    }

    public boolean isResultHasNext()
    {
        return resultHasNext;
    }

    public void setResultHasNext(boolean resultHasNext)
    {
        this.resultHasNext = resultHasNext;
    }

    public ResultStatus getStatus()
    {
        return resultStatus;
    }

    public int getResultSetNum()
    {
        return resultSetNum;
    }

    public void setJdbcResultSet(ResultSet jdbcResultSet)
    {
        this.jdbcResultSet = jdbcResultSet;
    }

    public List<Column> getSchema()
    {
        return this.schema;
    }

    public boolean addBlock(Block block)
    {
        if (currentBlocks.size() >= capacity) {
            return false;
        }
        currentBlocks.add(block);
        return true;
    }

    public boolean hasNext()
    {
        if (currentBlocks.size() > 0) {
            return true;
        }
        return false;
    }

    /**
     * Get next block in result set.
     * @return null if no block available.
     * */
    public Block getNext()
    {
        if (currentBlocks.size() == 0) {
            fetch();
        }
        if (currentBlocks.size() == 0) {
            return null;
        }
        return currentBlocks.remove(currentBlocks.size() - 1);
    }

    private void fetch()
    {
        if (jdbcResultSet != null) {
            Block block = new Block();
            try {
                while (jdbcResultSet.next() && currentBlocks.size() < this.capacity) {
                    RowConstructor rowConstructor = new RowConstructor();
                    for (int i = 0; i < schema.size(); i++) {
                        Column column = schema.get(i);
                        if (column.getDataType() == DataType.CHAR.getType()
                                || column.getDataType() == DataType.VARCHAR.getType()
                                || column.getDataType() == DataType.DATE.getType()) {
                            rowConstructor.appendString(jdbcResultSet.getString(i + 1));
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
                    if (!block.addRow(row)) {
                        if (!addBlock(block)) {
                            this.currentRow = row;
                            return;
                        }
                        block = new Block();
                        block.addRow(row);
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
                .toString();
    }
}
