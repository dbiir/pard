package cn.edu.ruc.iir.pard.sql.tree;

/**
 * pard
 *
 * @author guodong
 */
public abstract class Relation
    extends Node
{
    protected Relation(Location location)
    {
        super(location);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitRelation(this, context);
    }
}
