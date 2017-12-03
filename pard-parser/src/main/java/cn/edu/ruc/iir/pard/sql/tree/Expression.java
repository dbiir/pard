package cn.edu.ruc.iir.pard.sql.tree;

/**
 * pard
 *
 * @author guodong
 */
public abstract class Expression
        extends Node
{
    protected Expression(Location location)
    {
        super(location);
    }

    @Override
    protected <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitExpression(this, context);
    }

    @Override
    public final String toString()
    {
        return "";
    }
}
