package cn.edu.ruc.iir.pard.sql.tree;

import java.util.List;

/**
 * pard
 *
 * @author guodong
 */
public final class ListPartitionElement
        extends Node
{
    protected ListPartitionElement(Location location)
    {
        super(location);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return null;
    }

    @Override
    public int hashCode()
    {
        return 0;
    }

    @Override
    public boolean equals(Object obj)
    {
        return false;
    }

    @Override
    public String toString()
    {
        return null;
    }
}
