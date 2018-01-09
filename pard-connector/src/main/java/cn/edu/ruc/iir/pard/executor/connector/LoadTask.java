package cn.edu.ruc.iir.pard.executor.connector;

import java.util.List;

/**
 * pard
 *
 * @author guodong
 */
public class LoadTask
        extends Task
{
    private static final long serialVersionUID = 8929262500361094877L;

    private final String schema;
    private final String table;
    private final List<String> paths;

    public LoadTask(String schema, String table, List<String> paths)
    {
        this(schema, table, paths, null);
    }

    public LoadTask(String schema, String table, List<String> paths, String site)
    {
        super(site);
        this.schema = schema;
        this.table = table;
        this.paths = paths;
    }

    public String getSchema()
    {
        return schema;
    }

    public String getTable()
    {
        return table;
    }

    public List<String> getPaths()
    {
        return paths;
    }
}
