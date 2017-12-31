package cn.edu.ruc.iir.pard.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * pard
 *
 * @author guodong
 */
public class RangePartitionElementCondition
        extends Node
{
    public enum Predicate {
        LESS, GREATER, LESSEQ, GREATEREQ, EQUAL, NULL
    }

    private final Identifier partitionColumn;
    private final Predicate partitionPredicate;
    private final Expression partitionExpr;
    private final boolean minValue;
    private final boolean maxValue;

    public RangePartitionElementCondition(Identifier partitionColumn, Predicate partitionPredicate, Expression partitionExpr, boolean minValue, boolean maxValue)
    {
        this(null, partitionColumn, partitionPredicate, partitionExpr, minValue, maxValue);
    }

    public RangePartitionElementCondition(Location location, Identifier partitionColumn, Predicate partitionPredicate, Expression partitionExpr, boolean minValue, boolean maxValue)
    {
        super(location);
        this.partitionColumn = partitionColumn;
        this.partitionPredicate = partitionPredicate;
        this.partitionExpr = partitionExpr;
        this.minValue = minValue;
        this.maxValue = maxValue;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitRangePartitionElementCondition(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.of(partitionExpr);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(partitionColumn, partitionPredicate, partitionExpr, maxValue, maxValue);
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
        RangePartitionElementCondition that = (RangePartitionElementCondition) o;

        return Objects.equals(partitionColumn, that.partitionColumn) &&
                Objects.equals(partitionPredicate, that.partitionPredicate) &&
                Objects.equals(partitionExpr, that.partitionExpr) &&
                Objects.equals(minValue, that.minValue) &&
                Objects.equals(maxValue, that.maxValue);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("partition column", partitionColumn.getValue())
                .add("partition predicate", partitionPredicate.toString())
                .add("partition expression", partitionExpr == null ? "" : partitionExpr.toString())
                .add("minValue", minValue)
                .add("maxValue", maxValue)
                .toString();
    }

    public Identifier getPartitionColumn()
    {
        return partitionColumn;
    }

    public Predicate getPartitionPredicate()
    {
        return partitionPredicate;
    }

    public Expression getPartitionExpr()
    {
        return partitionExpr;
    }

    public boolean isMinValue()
    {
        return minValue;
    }

    public boolean isMaxValue()
    {
        return maxValue;
    }
}
