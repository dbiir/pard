package cn.edu.ruc.iir.pard.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

/**
 * pard
 *
 * @author guodong
 */
public class SubqueryExpression
        extends Expression
{
    private final Query query;

    public SubqueryExpression(Query query)
    {
        this(null, query);
    }

    public SubqueryExpression(Location location, Query query)
    {
        super(location);
        this.query = query;
    }

    public Query getQuery()
    {
        return query;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitSubqueryExpression(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of(query);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SubqueryExpression that = (SubqueryExpression) o;
        return Objects.equals(query, that.query);
    }

    @Override
    public int hashCode()
    {
        return query.hashCode();
    }
}
