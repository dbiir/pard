package cn.edu.ruc.iir.pard.executor.connector.node;

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
}
