package cn.edu.ruc.iir.pard.commons.memory;

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
    private final List<String> names;
    private final List<Integer> types;
    private final int capacity;
    private final int sequenceId;
    private boolean sequenceHasNext = false;
    private int currentSize = 0;

    public Block(List<String> names, List<Integer> types, int capacity, int sequenceId)
    {
        this.rows = new ArrayList<>();
        this.names = names;
        this.types = types;
        this.capacity = capacity;
        this.sequenceId = sequenceId;
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

    public List<String> getNames()
    {
        return names;
    }

    public List<Integer> getTypes()
    {
        return types;
    }

    public int getSequenceId()
    {
        return sequenceId;
    }

    public boolean isSequenceHasNext()
    {
        return sequenceHasNext;
    }
}
