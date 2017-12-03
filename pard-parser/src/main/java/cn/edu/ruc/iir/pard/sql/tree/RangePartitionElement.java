package cn.edu.ruc.iir.pard.sql.tree;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * pard
 *
 * @author guodong
 */
public final class RangePartitionElement
        extends Node
{
    private final Identifier partitionName;
    private final List<RangePartitionElementCondition> conditions;
    private String nodeId;

    public RangePartitionElement(Identifier partitionName, List<RangePartitionElementCondition> conditions)
    {
        this(null, partitionName, conditions, null);
    }

    public RangePartitionElement(Identifier partitionName, List<RangePartitionElementCondition> conditions, String nodeId)
    {
        this(null, partitionName, conditions, nodeId);
    }

    public RangePartitionElement(Location location, Identifier partitionName, List<RangePartitionElementCondition> conditions, String nodeId)
    {
        super(location);
        this.partitionName = partitionName;
        this.conditions = conditions;
        this.nodeId = nodeId;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitRangePartitionElement(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return conditions;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(partitionName, conditions, nodeId);
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

        RangePartitionElement that = (RangePartitionElement) o;
        return Objects.equals(partitionName, that.partitionName) &&
                Objects.equals(conditions, that.conditions) &&
                Objects.equals(nodeId, that.nodeId);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("partition name", partitionName)
                .add("conditions", conditions)
                .add("node", nodeId == null ? "" : nodeId)
                .toString();
    }
}
