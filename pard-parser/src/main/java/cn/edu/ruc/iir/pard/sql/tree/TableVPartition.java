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
public final class TableVPartition
    extends Statement
{
    private final List<TableElement> elements;

    public TableVPartition(List<TableElement> elements)
    {
        this(null, elements);
    }

    public TableVPartition(Location location, List<TableElement> elements)
    {
        super(location);
        this.elements = ImmutableList.copyOf(requireNonNull(elements, "elements is null"));
    }

    public List<TableElement> getElements()
    {
        return elements;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitTableVPartition(this, context);
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
        TableVPartition o = (TableVPartition) obj;
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
