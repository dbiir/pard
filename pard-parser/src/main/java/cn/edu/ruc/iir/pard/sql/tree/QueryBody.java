package cn.edu.ruc.iir.pard.sql.tree;

/**
 * pard
 *
 * @author guodong
 */
public abstract class QueryBody
        extends Relation
{
    protected QueryBody(Location location)
    {
        super(location);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitQueryBody(this, context);
    }
}
