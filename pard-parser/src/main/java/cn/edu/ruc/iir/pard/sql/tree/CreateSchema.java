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
public final class CreateSchema
        extends Statement
{
    private static final long serialVersionUID = -5795732084505036333L;
    private final QualifiedName schemaName;
    private final boolean notExists;

    public CreateSchema(QualifiedName schemaName, boolean notExists)
    {
        this(null, schemaName, notExists);
    }

    public CreateSchema(Location location, QualifiedName schemaName, boolean notExists)
    {
        super(location);
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.notExists = notExists;
    }

    public QualifiedName getSchemaName()
    {
        return schemaName;
    }

    public boolean isNotExists()
    {
        return notExists;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitCreateSchema(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return null;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaName, notExists);
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
        CreateSchema o = (CreateSchema) obj;
        return Objects.equals(schemaName, o.schemaName) &&
                Objects.equals(notExists, o.notExists);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("schemaName", schemaName)
                .add("notExists", notExists)
                .toString();
    }
}
