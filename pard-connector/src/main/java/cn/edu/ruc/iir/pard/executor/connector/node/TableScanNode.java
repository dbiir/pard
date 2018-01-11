package cn.edu.ruc.iir.pard.executor.connector.node;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * pard
 *
 * @author guodong
 */
public class TableScanNode
        extends InputNode
{
    private static final long serialVersionUID = 2673717961909269975L;
    private final String schema;
    private final String table;
    private String site;
/*
    private Optional<LimitNode> limit;
    private Optional<SortNode> sort;
    private Optional<DistinctNode> distinct;
    private ProjectNode project;
    private Optional<FilterNode> filter;
    */
    public TableScanNode(String schema, String table)
    {
        /*
        this.limit = Optional.ofNullable(null);
        this.sort = Optional.ofNullable(null);
        this.distinct = Optional.ofNullable(null);
        this.project = null;
        this.filter = Optional.ofNullable(null);
        */
        this.name = "TABLESCAN";
        this.schema = schema;
        this.table = table;
    }

    public TableScanNode(String schema, String table, String site)
    {
        /*
        this.limit = Optional.ofNullable(null);
        this.sort = Optional.ofNullable(null);
        this.distinct = Optional.ofNullable(null);
        this.project = null;
        this.filter = Optional.ofNullable(null);
        */
        this.name = "TABLESCAN";
        this.schema = schema;
        this.table = table;
        this.site = site;
    }
    public String getSchema()
    {
        return schema;
    }

    public String getTable()
    {
        return table;
    }

    public String getSite()
    {
        return site;
    }

    public void setSite(String site)
    {
        this.site = site;
    }
/*
    public Optional<LimitNode> getLimit()
    {
        return limit;
    }

    public void setLimit(LimitNode limit)
    {
        this.limit = Optional.of(limit);
    }

    public Optional<SortNode> getSort()
    {
        return sort;
    }

    public void setSort(SortNode sort)
    {
        this.sort = Optional.of(sort);
    }

    public Optional<DistinctNode> getDistinct()
    {
        return distinct;
    }

    public void setDistinct(DistinctNode distinct)
    {
        this.distinct = Optional.of(distinct);
    }

    public ProjectNode getProject()
    {
        return project;
    }

    public void setProject(ProjectNode project)
    {
        this.project = project;
    }

    public Optional<FilterNode> getFilter()
    {
        return filter;
    }

    public void setFilter(FilterNode filter)
    {
        this.filter = Optional.of(filter);
    }
*/
    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", "TABLESCAN")
                .add("schema", schema)
                .add("table", table)
                .add("site", site)
                .add("child", getLeftChild())
                .toString();
    }
}
