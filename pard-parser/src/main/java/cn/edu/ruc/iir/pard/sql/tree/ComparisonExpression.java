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
public class ComparisonExpression
        extends Expression
{
    private final ComparisonExpressionType type;
    private final Expression left;
    private final Expression right;

    public ComparisonExpression(ComparisonExpressionType type, Expression left, Expression right)
    {
        this(null, type, left, right);
    }

    public ComparisonExpression(Location location, ComparisonExpressionType type, Expression left, Expression right)
    {
        super(location);
        requireNonNull(type, "type is null");
        requireNonNull(left, "left is null");
        requireNonNull(right, "right is null");

        this.type = type;
        this.left = left;
        this.right = right;
    }

    public ComparisonExpressionType getType()
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
        return visitor.visitComparisonExpression(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of(left, right);
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

        ComparisonExpression that = (ComparisonExpression) o;
        return (type == that.type) &&
                Objects.equals(left, that.left) &&
                Objects.equals(right, that.right);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(type, left, right);
    }
}
