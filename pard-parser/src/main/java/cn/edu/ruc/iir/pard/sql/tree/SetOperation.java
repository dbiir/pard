package cn.edu.ruc.iir.pard.sql.tree;

import java.util.List;

/**
 * pard
 *
 * @author guodong
 */
public abstract class SetOperation
        extends QueryBody
{
    private final boolean distinct;

    protected SetOperation(Location location, boolean distinct)
    {
        super(location);
        this.distinct = distinct;
    }

    public boolean isDistinct()
    {
        return distinct;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitSetOperation(this, context);
    }

    public abstract List<Relation> getRelations();
}
