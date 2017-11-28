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
public final class TableHListPartitioner
    extends TableHPartitioner
{
    private final List<ListPartitionElement> elements;

    public TableHListPartitioner(List<ListPartitionElement> elements)
    {
        this(null, elements);
    }

    public TableHListPartitioner(Location location, List<ListPartitionElement> elements)
    {
        super(location);
        this.elements = ImmutableList.copyOf(requireNonNull(elements, "elements is null"));
    }

    public List<ListPartitionElement> getElements()
    {
        return elements;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitTableHListPartition(this, context);
    }

    @Override
    public List<ListPartitionElement> getChildren()
    {
        return elements;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(elements);
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
        TableHListPartitioner o = (TableHListPartitioner) obj;
        return Objects.equals(elements, o.elements);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("elements", elements)
                .toString();
    }
}
