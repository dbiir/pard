package cn.edu.ruc.iir.pard.commons.memory;

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
    private static final int defaultCapacity = 10 * 1024 * 1024;       // default to 10MB

    private final int capacity;                        // capacity in bytes
    private final List<Row> rows;                      // block content

    private int currentCap = 0;

    public Block()
    {
        this(defaultCapacity);
    }

    public Block(int capacity)
    {
        this.capacity = capacity;
        this.rows = new LinkedList<>();
    }

    /**
     * @return if added successfully
     * */
    public boolean addRow(Row row)
    {
        // todo check block capacity
        if (currentCap + row.getSize() >= capacity) {
            return false;
        }
        rows.add(row);
        currentCap += row.getSize();
        return true;
    }

    public boolean hasNext()
    {
        return rows.size() > 0;
    }

    public Row getNext()
    {
        return rows.remove(rows.size() - 1);
    }

    public int getRowNum()
    {
        return this.rows.size();
    }
}
