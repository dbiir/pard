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
public class Load
        extends Statement
{
    private static final long serialVersionUID = 7933595140893095119L;
    private final Identifier path;
    private final QualifiedName table;

    public Load(Identifier path, QualifiedName table)
    {
        this(null, path, table);
    }

    public Load(Location location, Identifier path, QualifiedName table)
    {
        super(location);
        this.path = path;
        this.table = table;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitLoad(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(path, table);
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
        Load o = (Load) obj;
        return Objects.equals(path, o.path) &&
                Objects.equals(table, o.table);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("path", path)
                .add("table", table)
                .omitNullValues()
                .toString();
    }
}
