package cn.edu.ruc.iir.pard.sql.tree;

import java.util.List;

/**
 * pard
 *
 * @author guodong
 */
public class Delete
        extends Statement
{
    private final QualifiedName name;
    private final Expression expression;

    public Delete(QualifiedName name, Expression expression)
    {
        this(null, name, expression);
    }

    public Delete(Location location, QualifiedName name, Expression expression)
    {
        super(location);
        this.name = name;
        this.expression = expression;
    }

    public QualifiedName getName()
    {
        return name;
    }

    public Expression getExpression()
    {
        return expression;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitDelete(this, context);
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
