package cn.edu.ruc.iir.pard.sql.tree;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * pard
 *
 * @author guodong
 */
public class Insert
        extends Statement
{
    private final QualifiedName target;
    private final Query query;
    private final Optional<List<String>> columns;

    public Insert(QualifiedName target, Optional<List<String>> columns, Query query)
    {
        this(null, target, columns, query);
    }

    public Insert(Location location, QualifiedName target, Optional<List<String>> columns, Query query)
    {
        super(location);
        this.target = target;
        this.columns = columns;
        this.query = query;
    }

    public QualifiedName getTarget()
    {
        return target;
    }

    public Query getQuery()
    {
        return query;
    }

    public Optional<List<String>> getColumns()
    {
        return columns;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitInsert(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return null;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(target, columns, query);
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
        Insert o = (Insert) obj;
        return Objects.equals(target, o.target) &&
                Objects.equals(columns, o.columns) &&
                Objects.equals(query, o.query);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("target", target)
                .add("columns", columns)
                .add("query", query)
                .toString();
    }
}
