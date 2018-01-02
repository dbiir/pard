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
    private final String schema;
    private final String table;
    private String site;

    public TableScanNode(String schema, String table)
    {
        this.schema = schema;
        this.table = table;
    }

    public TableScanNode(String schema, String table, String site)
    {
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
