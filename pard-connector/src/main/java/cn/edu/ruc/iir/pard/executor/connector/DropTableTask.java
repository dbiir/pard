package cn.edu.ruc.iir.pard.executor.connector;

/**
 * pard
 *
 * @author guodong
 */
public class DropTableTask
        extends Task
{
    private static final long serialVersionUID = 2879688099244307837L;
    private final String schemaName;
    private final String tableName;

    public DropTableTask(String schemaName, String tableName)
    {
        this(schemaName, tableName, null);
    }

    public DropTableTask(String schemaName, String tableName, String site)
    {
        super(site);
        this.schemaName = schemaName;
        this.tableName = tableName;
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
