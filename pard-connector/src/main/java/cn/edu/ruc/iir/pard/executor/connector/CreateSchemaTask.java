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
        this.schemaName = schemaName;
        this.isNotExists = isNotExists;
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
