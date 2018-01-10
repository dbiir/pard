package cn.edu.ruc.iir.pard.sql.tree;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * pard
 *
 * @author guodong
 */
public class ShowTables
        extends Statement
{
    private static final long serialVersionUID = -5343733186196510934L;
    private final Identifier schema;

    public ShowTables()
    {
        this(null, null);
    }

    public ShowTables(Location location)
    {
        this(location, null);
    }

    public ShowTables(Identifier schema)
    {
        this(null, schema);
    }

    public ShowTables(Location location, Identifier schema)
    {
        super(location);
        this.schema = schema;
    }

    public Identifier getSchema()
    {
        return schema;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitShowTables(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return null;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schema);
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
        ShowTables o = (ShowTables) obj;
        return Objects.equals(schema, o.schema);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("schema", schema)
                .omitNullValues()
                .toString();
    }
}
