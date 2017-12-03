package cn.edu.ruc.iir.pard.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * pard
 *
 * @author guodong
 */
public final class OrderBy
        extends Node
{
    private final List<SortItem> sortItems;

    public OrderBy(List<SortItem> sortItems)
    {
        this(null, sortItems);
    }

    public OrderBy(Location location, List<SortItem> sortItems)
    {
        super(location);
        requireNonNull(sortItems, "sortItems is null");
        checkArgument(!sortItems.isEmpty(), "sortItems should not be empty");
        this.sortItems = ImmutableList.copyOf(sortItems);
    }
    public List<SortItem> getSortItems()
    {
        return sortItems;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitOrderBy(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return sortItems;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("sortItems", sortItems)
                .toString();
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
        OrderBy o = (OrderBy) obj;
        return Objects.equals(sortItems, o.sortItems);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(sortItems);
    }
}
