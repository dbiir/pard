package cn.edu.ruc.iir.pard.executor.connector;

import cn.edu.ruc.iir.pard.catalog.Column;

import java.util.List;

/**
 * pard
 *
 * @author guodong
 */
public class CreateTableTask
        extends Task
{
    private static final long serialVersionUID = 5102186316038224483L;
    private final String schemaName;
    private final String tableName;
    private final boolean isNotExists;
    private final List<Column> columnDefinitions;

    public CreateTableTask(
            String schemaName,
            String tableName,
            boolean isNotExists,
            List<Column> columnDefinitions)
    {
        this(schemaName, tableName, isNotExists, columnDefinitions, null);
    }

    public CreateTableTask(
            String schemaName,
            String tableName,
            boolean isNotExists,
            List<Column> columnDefinitions,
            String site)
    {
        super(site);
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.isNotExists = isNotExists;
        this.columnDefinitions = columnDefinitions;
    }

    public String getSchemaName()
    {
        return schemaName;
    }

    public String getTableName()
    {
        return tableName;
    }

    public boolean isNotExists()
    {
        return isNotExists;
    }

    public List<Column> getColumnDefinitions()
    {
        return columnDefinitions;
    }
}
