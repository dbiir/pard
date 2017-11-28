package cn.edu.ruc.iir.pard.sql.tree;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;

/**
 * pard
 *
 * @author guodong
 */
public final class Limit
    extends Node
{
    private int limitNum;

    public Limit(int limitNum)
    {
        this(null, limitNum);
    }

    public Limit(Location location, int limitNum)
    {
        super(location);
        checkArgument(limitNum >= 0, "limit is less than 0");
        this.limitNum = limitNum;
    }

    public int getLimitNum()
    {
        return limitNum;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitLimit(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return null;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("limit", limitNum)
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
        Limit o = (Limit) obj;
        return Objects.equals(limitNum, o.limitNum);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(limitNum);
    }
}
