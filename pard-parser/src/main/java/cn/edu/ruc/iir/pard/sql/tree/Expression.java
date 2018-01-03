package cn.edu.ruc.iir.pard.sql.tree;

import cn.edu.ruc.iir.pard.sql.utils.ExpressionFormatter;

import java.io.Serializable;
import java.util.Optional;

/**
 * pard
 *
 * @author guodong
 */
public abstract class Expression
        extends Node implements Serializable
{
    private static final long serialVersionUID = -7937179405838230298L;

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
        return ExpressionFormatter.formatExpression(this, Optional.empty());
    }
}
