package cn.edu.ruc.iir.pard.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * pard
 *
 * @author guodong
 */
public class LogicalBinaryExpression
        extends Expression
{
    public enum Type
    {
        AND, OR;

        public Type flip()
        {
            switch (this) {
                case AND:
                    return LogicalBinaryExpression.Type.OR;
                case OR:
                    return LogicalBinaryExpression.Type.AND;
                default:
                    throw new IllegalArgumentException("Unsupported logical expression type: " + this);
            }
        }
    }

    private final Type type;
    private final Expression left;
    private final Expression right;

    public LogicalBinaryExpression(Type type, Expression left, Expression right)
    {
        this(null, type, left, right);
    }

    public LogicalBinaryExpression(Location location, Type type, Expression left, Expression right)
    {
        super(location);
        requireNonNull(type, "type is null");
        requireNonNull(left, "left is null");
        requireNonNull(right, "right is null");

        this.type = type;
        this.left = left;
        this.right = right;
    }

    public Type getType()
    {
        return type;
    }

    public Expression getLeft()
    {
        return left;
    }

    public Expression getRight()
    {
        return right;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitLogicalBinaryExpression(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of(left, right);
    }

    public static LogicalBinaryExpression and(Expression left, Expression right)
    {
        return new LogicalBinaryExpression(null, Type.AND, left, right);
    }

    public static LogicalBinaryExpression or(Expression left, Expression right)
    {
        return new LogicalBinaryExpression(null, Type.OR, left, right);
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

        LogicalBinaryExpression that = (LogicalBinaryExpression) o;
        return type == that.type &&
                Objects.equals(left, that.left) &&
                Objects.equals(right, that.right);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(type, left, right);
    }
}
