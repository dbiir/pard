package cn.edu.ruc.iir.pard.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * pard
 *
 * @author guodong
 */
public class NotExpression
        extends Expression
{
    private final Expression value;

    public NotExpression(Expression value)
    {
        this(null, value);
    }

    public NotExpression(Location location, Expression value)
    {
        super(location);
        requireNonNull(value, "value is null");
        this.value = value;
    }

    public Expression getValue()
    {
        return value;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitNotExpression(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of(value);
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

        NotExpression that = (NotExpression) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode()
    {
        return value.hashCode();
    }
}
