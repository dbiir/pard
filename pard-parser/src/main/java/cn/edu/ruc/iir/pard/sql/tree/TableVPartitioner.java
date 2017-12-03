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

    public TableVPartitioner(List<TableElement> elements)
    {
        this(null, elements);
    }

    public TableVPartitioner(Location location, List<TableElement> elements)
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
        TableVPartitioner o = (TableVPartitioner) obj;
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
