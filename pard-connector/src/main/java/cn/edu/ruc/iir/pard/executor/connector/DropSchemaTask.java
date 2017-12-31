package cn.edu.ruc.iir.pard.executor.connector;

/**
 * pard
 *
 * @author guodong
 */
public class DropSchemaTask
        extends Task
{
    private static final long serialVersionUID = 9196504080983998135L;
    private final String schema;
    private final boolean isExists;

    public DropSchemaTask(String schema, boolean isExists)
    {
        this(schema, isExists, null);
    }

    public DropSchemaTask(String schema, boolean isExists, String site)
    {
        super(site);
        this.schema = schema;
        this.isExists = isExists;
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
