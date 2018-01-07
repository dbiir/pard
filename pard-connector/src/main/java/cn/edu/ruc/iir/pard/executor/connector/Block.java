package cn.edu.ruc.iir.pard.executor.connector;

import cn.edu.ruc.iir.pard.catalog.Column;
import cn.edu.ruc.iir.pard.commons.memory.Row;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * pard
 *
 * @author guodong
 */
public class Block
        implements Serializable
{
    private static final long serialVersionUID = -1088925471100838929L;

    private final List<Row> rows;
    private final List<Column> columns;
    private final int capacity;
    private final int sequenceId;
    private final String taskId;
    private boolean sequenceHasNext = false;
    private int currentSize = 0;

    public Block(List<Column> columns, int capacity, int sequenceId, String taskId)
    {
        this.rows = new ArrayList<>();
        this.columns = columns;
        this.capacity = capacity;
        this.sequenceId = sequenceId;
        this.taskId = taskId;
    }

    public boolean addRow(Row row)
    {
        this.rows.add(row);
        currentSize += row.getSize();
        return currentSize <= capacity;
    }

    public void setSequenceHasNext(boolean sequenceHasNext)
    {
        this.sequenceHasNext = sequenceHasNext;
    }

    public List<Row> getRows()
    {
        return rows;
    }

    public List<Column> getColumns()
    {
        return columns;
    }

    public int getSequenceId()
    {
        return sequenceId;
    }

    public String getTaskId()
    {
        return taskId;
    }

    public boolean isSequenceHasNext()
    {
        return sequenceHasNext;
    }
}
