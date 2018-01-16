package cn.edu.ruc.iir.pard.executor.connector;

import cn.edu.ruc.iir.pard.catalog.Column;

import java.util.List;

public class CreateTmpTableTask
        extends Task
{
    private static final long serialVersionUID = 5L;
    private final String schemaName;
    private final String tableName;
    private final List<Column> columnDefinitions;
    private final String path;

    public CreateTmpTableTask(String schemaName, String tableName, List<Column> columnDefinitions, String path)
    {
        this(schemaName, tableName, columnDefinitions, path, null);
    }

    public CreateTmpTableTask(String schemaName, String tableName, List<Column> columnDefinitions, String path, String site)
    {
        super(site);
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.columnDefinitions = columnDefinitions;
        this.path = path;
    }

    public String getSchemaName()
    {
        return schemaName;
    }

    public String getTableName()
    {
        return tableName;
    }

    public List<Column> getColumnDefinitions()
    {
        return columnDefinitions;
    }

    public String getPath()
    {
        return path;
    }
}
