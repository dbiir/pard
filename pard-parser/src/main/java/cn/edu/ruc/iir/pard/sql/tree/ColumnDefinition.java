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
public final class ColumnDefinition
        extends TableElement
{
    private final Identifier name;
    private final String type;
    private final boolean primary;

    public ColumnDefinition(Identifier name, String type, boolean primary)
    {
        this(null, name, type, primary);
    }

    public ColumnDefinition(Location location, Identifier name, String type, boolean primary)
    {
        super(location);
        this.name = requireNonNull(name, "name is null");
        this.type = requireNonNull(type, "type is null");
        this.primary = primary;
    }

    public Identifier getName()
    {
        return name;
    }

    public String getType()
    {
        return type;
    }

    public boolean getPrimary()
    {
        return primary;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitColumnDefinition(this, context);
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
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ColumnDefinition o = (ColumnDefinition) obj;
        return Objects.equals(this.name, o.name) &&
                Objects.equals(this.type, o.type) &&
                Objects.equals(this.primary, o.primary);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, type, primary);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name.getValue())
                .add("type", type)
                .add("primary", primary)
                .toString();
    }
}
