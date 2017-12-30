package cn.edu.ruc.iir.pard.executor.connector;

/**
 * pard
 *
 * @author guodong
 */
public class DropSchemaTask
        extends Task
{
    private final String schema;
    private final boolean isExists;

    public DropSchemaTask(String schema, boolean isExists)
    {
        this(schema, isExists, null);
    }

    public DropSchemaTask(String schema, boolean isExists, String site)
    {
        this.schema = schema;
        this.isExists = isExists;
        this.site = site;
    }

    public String getSchema()
    {
        return schema;
    }

    public boolean isExists()
    {
        return isExists;
    }
}
