package cn.edu.ruc.iir.pard.executor.connector;

/**
 * pard
 *
 * @author guodong
 */
public class DropTableTask
        extends Task
{
    private final String schemaName;
    private final String tableName;

    public DropTableTask(String schemaName, String tableName)
    {
        this(schemaName, tableName, null);
    }

    public DropTableTask(String schemaName, String tableName, String site)
    {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.site = site;
    }

    public String getSchemaName()
    {
        return schemaName;
    }

    public String getTableName()
    {
        return tableName;
    }
}
