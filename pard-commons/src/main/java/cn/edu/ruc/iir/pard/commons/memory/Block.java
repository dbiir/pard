package cn.edu.ruc.iir.pard.commons.memory;

import com.google.common.collect.ImmutableList;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

/**
 * pard
 *
 * @author guodong
 */
public class Block
        implements Serializable
{
    private static final long serialVersionUID = 4306066541526735236L;
    private final ImmutableList<String> columnNames;   // column names in schema
    private final ImmutableList<String> columnTypes;   // column types in schema
    private final int capacity;                        // capacity in bytes
    private final List<Row> rows;                      // block content

    private int currentCap = 0;

    public Block(List<String> columnNames, List<String> columnTypes, int capacity)
    {
        this.columnNames = ImmutableList.copyOf(columnNames);
        this.columnTypes = ImmutableList.copyOf(columnTypes);
        this.capacity = capacity;
        this.rows = new LinkedList<>();
    }

    /**
     * @return if added successfully
     * */
    public boolean addRow(Row row)
    {
        if (currentCap >= capacity) {
            return false;
        }
        rows.add(row);
        return true;
    }

    public boolean hasNext()
    {
        return rows.size() > 0;
    }

    public Row getNext()
    {
        return rows.remove(rows.size());
    }

    public int getRowSize()
    {
        return this.rows.size();
    }
}
