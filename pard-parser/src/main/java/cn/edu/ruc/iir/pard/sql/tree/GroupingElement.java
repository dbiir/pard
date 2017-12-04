package cn.edu.ruc.iir.pard.sql.tree;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Objects;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * pard
 *
 * @author guodong
 */
public class GroupingElement
        extends Node
{
    private final List<Expression> columns;

    public GroupingElement(List<Expression> simpleGroupByExpressions)
    {
        this(null, simpleGroupByExpressions);
    }

    public GroupingElement(Location location, List<Expression> simpleGroupByExpressions)
    {
        super(location);
        this.columns = ImmutableList.copyOf(requireNonNull(simpleGroupByExpressions, "simpleGroupByExpressions is null"));
    }

    public List<Expression> getColumnExpressions()
    {
        return columns;
    }

    public List<Set<Expression>> enumerateGroupingSets()
    {
        return ImmutableList.of(ImmutableSet.copyOf(columns));
    }

    @Override
    protected <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitGroupingElement(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return columns;
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
        GroupingElement that = (GroupingElement) o;
        return Objects.equals(columns, that.columns);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columns);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("columns", columns)
                .toString();
    }
}
