package cn.edu.ruc.iir.pard.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * pard
 *
 * @author guodong
 */
public abstract class Literal
        extends Expression
{
    protected Literal(Location location)
    {
        super(location);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitLiteral(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of();
    }
}

