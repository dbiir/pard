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
public final class TableHRangePartitioner
    extends TableHPartitioner
{
    private List<RangePartitionElement> elements;

    public TableHRangePartitioner(List<RangePartitionElement> elements)
    {
        this(null, elements);
    }

    public TableHRangePartitioner(Location location, List<RangePartitionElement> elements)
    {
        super(location);
        this.elements = ImmutableList.copyOf(requireNonNull(elements, "elements is null"));
    }

    public List<RangePartitionElement> getElements()
    {
        return elements;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitTableHRangePartition(this, context);
    }

    @Override
    public List<RangePartitionElement> getChildren()
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
        TableHRangePartitioner o = (TableHRangePartitioner) obj;
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
