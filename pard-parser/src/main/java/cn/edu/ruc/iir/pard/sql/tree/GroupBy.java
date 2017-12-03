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
public class GroupBy
        extends Node
{
    private final boolean isDistinct;
    private final List<GroupingElement> groupingElements;

    public GroupBy(boolean isDistinct, List<GroupingElement> groupingElements)
    {
        this(null, isDistinct, groupingElements);
    }

    public GroupBy(Location location, boolean isDistinct, List<GroupingElement> groupingElements)
    {
        super(location);
        this.isDistinct = isDistinct;
        this.groupingElements = ImmutableList.copyOf(requireNonNull(groupingElements));
    }

    public boolean isDistinct()
    {
        return isDistinct;
    }

    public List<GroupingElement> getGroupingElements()
    {
        return groupingElements;
    }

    @Override
    protected <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitGroupBy(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return groupingElements;
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
        GroupBy groupBy = (GroupBy) o;
        return isDistinct == groupBy.isDistinct &&
                Objects.equals(groupingElements, groupBy.groupingElements);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(isDistinct, groupingElements);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("isDistinct", isDistinct)
                .add("groupingElements", groupingElements)
                .toString();
    }
}
