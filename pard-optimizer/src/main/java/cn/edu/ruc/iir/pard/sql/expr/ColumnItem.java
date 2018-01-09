package cn.edu.ruc.iir.pard.sql.expr;

public class ColumnItem
        extends Item
{
    private final String tableName;
    private final String columnName;
    private int dataType;

    public ColumnItem(ColumnItem ci)
    {
        super();
        this.tableName = ci.tableName;
        this.columnName = ci.columnName;
        this.dataType = ci.dataType;
    }
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
        return tableName + "." + columnName;
    }
    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((columnName == null) ? 0 : columnName.hashCode());
        result = prime * result + dataType;
        result = prime * result + ((tableName == null) ? 0 : tableName.hashCode());
        return result;
    }
    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ColumnItem other = (ColumnItem) obj;
        if (columnName == null) {
            if (other.columnName != null) {
                return false;
            }
        }
        else if (!columnName.equals(other.columnName)) {
            return false;
        }
        if (dataType != other.dataType) {
            return false;
        }
        if (tableName == null) {
            if (other.tableName != null) {
                return false;
            }
        }
        else if (!tableName.equals(other.tableName)) {
            return false;
        }
        return true;
    }
}
