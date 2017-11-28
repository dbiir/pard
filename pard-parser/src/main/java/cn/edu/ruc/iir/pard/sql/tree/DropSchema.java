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
public final class DropSchema
        extends Statement
{
    private final QualifiedName schemaName;
    private final boolean exists;
    private final boolean cascade;

    public DropSchema(QualifiedName schemaName, boolean exists, boolean cascade)
    {
        this(null, schemaName, exists, cascade);
    }

    public DropSchema(Location location, QualifiedName schemaName, boolean exists, boolean cascade)
    {
        super(location);
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.exists = exists;
        this.cascade = cascade;
    }

    public QualifiedName getSchemaName()
    {
        return schemaName;
    }

    public boolean isExists()
    {
        return exists;
    }

    public boolean isCascade()
    {
        return cascade;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitDropSchema(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of();
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
        DropSchema o = (DropSchema) obj;
        return Objects.equals(schemaName, o.schemaName) &&
                (exists == o.exists) &&
                (cascade == o.cascade);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaName, exists);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("schemaName", schemaName)
                .add("exists", exists)
                .add("cascade", cascade)
                .toString();
    }
}
