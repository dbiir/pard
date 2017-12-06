package cn.edu.ruc.iir.pard.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;
/**
 * pard
 *
 * @author guodong
 */
public final class Query
        extends Statement
{
    private final QueryBody queryBody;
    private final Optional<OrderBy> orderBy;
    private final Optional<String> limit;

    public Query(QueryBody queryBody)
    {
        this(null, queryBody, null, null);
    }

    public Query(QueryBody queryBody, OrderBy orderBy)
    {
        this(null, queryBody, orderBy, null);
    }

    public Query(QueryBody queryBody, String limit)
    {
        this(null, queryBody, null, limit);
    }

    public Query(QueryBody queryBody, OrderBy orderBy, String limit)
    {
        this(null, queryBody, orderBy, limit);
    }

    public Query(Location location, QueryBody queryBody, OrderBy orderBy, String limit)
    {
        super(location);
        this.queryBody = requireNonNull(queryBody, "query body is null");
        this.orderBy = orderBy == null ? Optional.empty() : Optional.of(orderBy);
        this.limit = limit == null ? Optional.empty() : Optional.of(limit);
    }

    public QueryBody getQueryBody()
    {
        return queryBody;
    }

    public Optional<OrderBy> getOrderBy()
    {
        return orderBy;
    }

    public Optional<String> getLimit()
    {
        return limit;
    }

    @Override
    public List<? extends Node> getChildren()
    {
        ImmutableList.Builder<Node> nodes = ImmutableList.builder();
        nodes.add(queryBody);
        orderBy.ifPresent(nodes::add);
        return nodes.build();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(queryBody, orderBy, limit);
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
        Query o = (Query) obj;
        return Objects.equals(queryBody, o.queryBody) &&
                Objects.equals(orderBy, o.orderBy) &&
                Objects.equals(limit, o.limit);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("queryBody", queryBody)
                .add("orderBy", orderBy)
                .add("limit", limit.orElse(null))
                .omitNullValues()
                .toString();
    }
}
