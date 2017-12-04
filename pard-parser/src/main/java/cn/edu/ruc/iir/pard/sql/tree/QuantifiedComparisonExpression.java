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
public class QuantifiedComparisonExpression
        extends Expression
{
    public enum Quantifier
    {
        ALL,
        ANY,
        SOME,
    }

    private final ComparisonExpressionType comparisonType;
    private final Quantifier quantifier;
    private final Expression value;
    private final Expression subquery;

    public QuantifiedComparisonExpression(ComparisonExpressionType comparisonType, Quantifier quantifier, Expression value, Expression subquery)
    {
        this(null, comparisonType, quantifier, value, subquery);
    }

    public QuantifiedComparisonExpression(Location location, ComparisonExpressionType comparisonType, Quantifier quantifier, Expression value, Expression subquery)
    {
        super(location);
        this.comparisonType = requireNonNull(comparisonType, "comparisonType is null");
        this.quantifier = requireNonNull(quantifier, "quantifier is null");
        this.value = requireNonNull(value, "value is null");
        this.subquery = requireNonNull(subquery, "subquery is null");
    }

    public ComparisonExpressionType getComparisonType()
    {
        return comparisonType;
    }

    public Quantifier getQuantifier()
    {
        return quantifier;
    }

    public Expression getValue()
    {
        return value;
    }

    public Expression getSubquery()
    {
        return subquery;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitQuantifiedComparisonExpression(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of(value, subquery);
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

        QuantifiedComparisonExpression that = (QuantifiedComparisonExpression) o;
        return comparisonType == that.comparisonType &&
                quantifier == that.quantifier &&
                Objects.equals(value, that.value) &&
                Objects.equals(subquery, that.subquery);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(comparisonType, quantifier, value, subquery);
    }
}
