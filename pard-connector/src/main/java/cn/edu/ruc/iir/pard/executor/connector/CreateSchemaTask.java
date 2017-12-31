package cn.edu.ruc.iir.pard.executor.connector;

/**
 * pard
 *
 * @author guodong
 */
public class CreateSchemaTask
        extends Task
{
    private final String schemaName;
    private final boolean isNotExists;

    public CreateSchemaTask(
            String schemaName,
            boolean isNotExists)
    {
        this(schemaName, isNotExists, null);
    }

    public CreateSchemaTask(
            String schemaName,
            boolean isNotExists,
            String site)
    {
        this.schemaName = schemaName;
        this.isNotExists = isNotExists;
        this.site = site;
    }

    public String getSchemaName()
    {
        return schemaName;
    }

    public boolean isNotExists()
    {
        return isNotExists;
    }
}
