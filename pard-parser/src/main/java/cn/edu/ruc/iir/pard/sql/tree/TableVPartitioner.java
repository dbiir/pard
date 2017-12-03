package cn.edu.ruc.iir.pard.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;
/**
 * pard
 *
 * @author guodong
 */
public final class TableVPartitioner
        extends Statement
{
    private final List<TableElement> elements;
    private String nodeId;

    public TableVPartitioner(List<TableElement> elements)
    {
        this(null, elements, null);
    }

    public TableVPartitioner(List<TableElement> elements, String nodeId)
    {
        this(null, elements, nodeId);
    }

    public TableVPartitioner(Location location, List<TableElement> elements, String nodeId)
    {
        super(location);
        this.elements = ImmutableList.copyOf(requireNonNull(elements, "elements is null"));
        this.nodeId = nodeId;
    }

    public List<TableElement> getElements()
    {
        return elements;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitTableVPartitioner(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.<Node>builder()
                .addAll(elements)
                .build();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(elements, nodeId);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        TableVPartitioner o = (TableVPartitioner) obj;
        return Objects.equals(elements, o.elements) &&
                Objects.equals(nodeId, o.nodeId);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("elements", elements)
                .add("node", nodeId == null ? "" : nodeId)
                .toString();
    }
}
