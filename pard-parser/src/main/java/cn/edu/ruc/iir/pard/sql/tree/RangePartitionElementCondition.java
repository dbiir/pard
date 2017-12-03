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
        LESS, GREATER, LESSEQ, GREATEREQ
    }

    private final Identifier partitionColumn;
    private final Predicate partitionPredicate;
    private final Expression partitionExpr;

    public RangePartitionElementCondition(Identifier partitionColumn, Predicate partitionPredicate, Expression partitionExpr)
    {
        this(null, partitionColumn, partitionPredicate, partitionExpr);
    }

    public RangePartitionElementCondition(Location location, Identifier partitionColumn, Predicate partitionPredicate, Expression partitionExpr)
    {
        super(location);
        this.partitionColumn = partitionColumn;
        this.partitionPredicate = partitionPredicate;
        this.partitionExpr = partitionExpr;
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
        return Objects.hash(partitionColumn, partitionPredicate, partitionExpr);
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
                Objects.equals(partitionExpr, that.partitionExpr);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("partition column", partitionColumn)
                .add("partition predicate", partitionPredicate)
                .add("partition expression", partitionExpr)
                .toString();
    }
}
