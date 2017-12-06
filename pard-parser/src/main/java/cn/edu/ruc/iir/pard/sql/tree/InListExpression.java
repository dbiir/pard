package cn.edu.ruc.iir.pard.sql.tree;

import java.util.List;
import java.util.Objects;

/**
 * pard
 *
 * @author guodong
 */
public class InListExpression
        extends Expression
{
    private final List<Expression> values;

    public InListExpression(List<Expression> values)
    {
        this(null, values);
    }

    public InListExpression(Location location, List<Expression> values)
    {
        super(location);
        this.values = values;
    }

    public List<Expression> getValues()
    {
        return values;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitInListExpression(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return values;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        InListExpression that = (InListExpression) o;
        return Objects.equals(values, that.values);
    }

    @Override
    public int hashCode()
    {
        return values.hashCode();
    }
}
