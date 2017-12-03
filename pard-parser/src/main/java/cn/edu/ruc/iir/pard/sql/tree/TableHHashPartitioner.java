package cn.edu.ruc.iir.pard.sql.tree;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;
/**
 * pard
 *
 * @author guodong
 */
public final class TableHHashPartitioner
        extends TableHPartitioner
{
    private final Expression keyExpr;
    private final int bucketNum;

    public TableHHashPartitioner(Expression keyExpr, int bucketNum)
    {
        this(null, keyExpr, bucketNum);
    }

    public TableHHashPartitioner(Location location, Expression keyExpr, int bucketNum)
    {
        super(location);
        this.keyExpr = requireNonNull(keyExpr, "key expression is null");
        this.bucketNum = bucketNum;
    }

    public Expression getKeyExpr()
    {
        return keyExpr;
    }

    public int getBucketNum()
    {
        return bucketNum;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitTableHHashPartitioner(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return null;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(keyExpr, bucketNum);
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
        TableHHashPartitioner o = (TableHHashPartitioner) obj;
        return Objects.equals(keyExpr, o.keyExpr) &&
                Objects.equals(bucketNum, o.bucketNum);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("key", keyExpr)
                .add("bucket num", bucketNum)
                .toString();
    }
}
