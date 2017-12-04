package cn.edu.ruc.iir.pard.sql.tree;

/**
 * pard
 *
 * @author guodong
 */
public abstract class TableHPartitioner
        extends Statement
{
    protected TableHPartitioner(Location location)
    {
        super(location);
    }

    @Override
    protected <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitTableHPartitioner(this, context);
    }
}
