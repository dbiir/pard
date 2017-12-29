package cn.edu.ruc.iir.pard.sql.expr;

public class ColumnItem
        extends Item
{
    private final String tableName;
    private final String columnName;
    private int dataType;

    public ColumnItem(String tableName, String columnName, int dataType)
    {
        super();
        this.tableName = tableName;
        this.columnName = columnName;
        this.dataType = dataType;
    }
    public String getTableName()
    {
        return tableName;
    }
    public String getColumnName()
    {
        return columnName;
    }
    public int getDataType()
    {
        return dataType;
    }
    @Override
    public String toString()
    {
        // TODO Auto-generated method stub
        return tableName + "." + columnName;
    }
}
