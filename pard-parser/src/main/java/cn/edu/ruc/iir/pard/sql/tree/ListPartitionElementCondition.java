package cn.edu.ruc.iir.pard.sql.tree;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * pard
 *
 * @author guodong
 */
public class ListPartitionElementCondition
        extends Node
{
    private final Identifier partitionColumn;
    private final List<Expression> inList;

    public ListPartitionElementCondition(Identifier partitionColumn, List<Expression> inList)
    {
        this(null, partitionColumn, inList);
    }

    public ListPartitionElementCondition(Location location, Identifier partitionColumn, List<Expression> inList)
    {
        super(location);
        this.partitionColumn = partitionColumn;
        this.inList = inList;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitListPartitionElementCondition(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return inList;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(partitionColumn, inList);
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

        ListPartitionElementCondition that = (ListPartitionElementCondition) o;
        return Objects.equals(partitionColumn, that.partitionColumn) &&
                Objects.equals(inList, that.inList);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("partition column", partitionColumn)
                .add("in list", inList)
                .toString();
    }
}
