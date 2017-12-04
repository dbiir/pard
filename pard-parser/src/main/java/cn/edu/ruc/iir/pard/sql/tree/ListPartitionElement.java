package cn.edu.ruc.iir.pard.sql.tree;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * pard
 *
 * @author guodong
 */
public final class ListPartitionElement
        extends Node
{
    private final Identifier partitionName;
    private final List<ListPartitionElementCondition> conditions;
    private String nodeId;

    public ListPartitionElement(Identifier partitionColumn, List<ListPartitionElementCondition> conditions)
    {
        this(null, partitionColumn, conditions, null);
    }

    public ListPartitionElement(Identifier partitionColumn, List<ListPartitionElementCondition> conditions, String nodeId)
    {
        this(null, partitionColumn, conditions, nodeId);
    }

    public ListPartitionElement(Location location, Identifier partitionColumn, List<ListPartitionElementCondition> conditions, String nodeId)
    {
        super(location);
        this.partitionName = partitionColumn;
        this.conditions = conditions;
        this.nodeId = nodeId;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitListPartitionElement(this, context);
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

        ListPartitionElement that = (ListPartitionElement) o;
        return Objects.equals(partitionName, that.partitionName) &&
                Objects.equals(conditions, that.conditions) &&
                Objects.equals(nodeId, that.nodeId);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("partition column", partitionName)
                .add("conditions", conditions)
                .add("node", nodeId == null ? "" : nodeId)
                .toString();
    }
}
