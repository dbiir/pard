package cn.edu.ruc.iir.pard.sql.tree;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * pard
 *
 * @author guodong
 */
public final class Row
        extends Expression
{
    private static final long serialVersionUID = -2352021238915447393L;
    private final List<Expression> items;

    public Row(List<Expression> items)
    {
        this(null, items);
    }

    public Row(Location location, List<Expression> items)
    {
        super(location);
        requireNonNull(items, "items is null");
        this.items = new ArrayList<>();
        this.items.addAll(items);
    }

    public List<Expression> getItems()
    {
        return items;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitRow(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return items;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(items);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        Row other = (Row) obj;
        return Objects.equals(this.items, other.items);
    }
}
