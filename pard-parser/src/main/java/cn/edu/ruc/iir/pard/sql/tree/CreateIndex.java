package cn.edu.ruc.iir.pard.sql.tree;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * pard
 *
 * @author guodong
 */
public class CreateIndex
        extends Node
{
    private final Identifier indexName;
    private final QualifiedName table;
    private final List<Identifier> columns;

    public CreateIndex(Identifier indexName, QualifiedName table, List<Identifier> columns)
    {
        this(null, indexName, table, columns);
    }

    public CreateIndex(Location location, Identifier indexName, QualifiedName table, List<Identifier> columns)
    {
        super(location);
        this.indexName = indexName;
        this.table = table;
        this.columns = columns;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitCreateIndex(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return null;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(indexName, table, columns);
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

        CreateIndex that = (CreateIndex) obj;

        return Objects.equals(indexName, that.indexName) &&
                Objects.equals(table, that.table) &&
                Objects.equals(columns, that.columns);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", indexName)
                .add("table", table)
                .add("columns", columns)
                .toString();
    }
}
