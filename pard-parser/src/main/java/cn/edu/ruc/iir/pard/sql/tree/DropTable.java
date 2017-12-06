package cn.edu.ruc.iir.pard.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * pard
 *
 * @author guodong
 */
public class DropTable
        extends Statement
{
    private final QualifiedName tableName;
    private final boolean exists;

    public DropTable(QualifiedName tableName, boolean exists)
    {
        this(null, tableName, exists);
    }

    public DropTable(Location location, QualifiedName tableName, boolean exists)
    {
        super(location);
        this.tableName = tableName;
        this.exists = exists;
    }

    public QualifiedName getTableName()
    {
        return tableName;
    }

    public boolean isExists()
    {
        return exists;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitDropTable(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tableName, exists);
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
        DropTable o = (DropTable) obj;
        return Objects.equals(tableName, o.tableName)
                && (exists == o.exists);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("tableName", tableName)
                .add("exists", exists)
                .toString();
    }
}
