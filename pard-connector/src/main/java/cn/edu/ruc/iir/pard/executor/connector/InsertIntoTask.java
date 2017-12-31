package cn.edu.ruc.iir.pard.executor.connector;

import cn.edu.ruc.iir.pard.catalog.Column;

import java.util.List;

/**
 * pard
 *
 * @author guodong
 */
public class InsertIntoTask
        extends Task
{
    private final String schemaName;
    private final String tableName;
    private final List<Column> columns;
    private final String[][] values;

    public InsertIntoTask(String schemaName, String tableName, List<Column> columns, String[][] values)
    {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.columns = columns;
        this.values = values;
    }

    public List<Column> getColumns()
    {
        return columns;
    }

    public String[][] getValues()
    {
        return values;
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
