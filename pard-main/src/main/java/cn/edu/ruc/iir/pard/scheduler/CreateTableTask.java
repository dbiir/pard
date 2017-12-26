package cn.edu.ruc.iir.pard.scheduler;

import cn.edu.ruc.iir.pard.sql.tree.ColumnDefinition;

import java.util.List;

/**
 * pard
 *
 * @author guodong
 */
public class CreateTableTask
        extends Task
{
    private final String schemaName;
    private final String tableName;
    private final boolean isNotExists;
    private final List<ColumnDefinition> columnDefinitions;

    public CreateTableTask(
            String schemaName,
            String tableName,
            boolean isNotExists,
            List<ColumnDefinition> columnDefinitions)
    {
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

    public List<ColumnDefinition> getColumnDefinitions()
    {
        return columnDefinitions;
    }
}
