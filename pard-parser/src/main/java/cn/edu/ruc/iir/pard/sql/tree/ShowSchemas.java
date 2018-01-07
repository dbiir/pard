package cn.edu.ruc.iir.pard.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * pard
 *
 * @author guodong
 */
public class ShowSchemas
        extends Statement
{
    private static final long serialVersionUID = 8417292548099984843L;

    public ShowSchemas(Location location)
    {
        super(location);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitShowSchemas(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    public int hashCode()
    {
        return 0;
    }

    @Override
    public boolean equals(Object obj)
    {
        return true;
    }

    @Override
    public String toString()
    {
        return "SHOW SCHEMAS";
    }
}
