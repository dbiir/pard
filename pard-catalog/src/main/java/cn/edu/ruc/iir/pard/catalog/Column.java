package cn.edu.ruc.iir.pard.catalog;

import java.io.Serializable;

public class Column
        implements Serializable
{
    private static final long serialVersionUID = -5890820616555548532L;
    private int id;
    private int dataType;
    private String columnName;
    private String tableName;
    private int len;
    private int index; //0:none; 1:hashindex; 2:btreeindex; 3:others
    private int key;

    public Column()
    {
    }

    public Column(int id, int dataType, String columnName, int len, int index, int key, String tableName)
    {
        this.id = id;
        this.dataType = dataType;
        this.columnName = columnName;
        this.len = len;
        this.index = index;
        this.key = key;
        this.tableName = tableName;
    }
    public Column(Column column)
    {
        this.id = column.id;
        this.dataType = column.dataType;
        this.columnName = column.columnName;
        this.len = column.len;
        this.index = column.index;
        this.key = column.key;
        this.tableName = column.tableName;
    }
    public int getId()
    {
        return id;
    }

    public int getDataType()
    {
        return dataType;
    }

    public String getColumnName()
    {
        return columnName;
    }

    public int getLen()
    {
        return len;
    }

    public int getIndex()
    {
        return index;
    }

    public int getKey()
    {
        return key;
    }

    public void setId(int id)
    {
        this.id = id;
    }

    public void setDataType(int dataType)
    {
        this.dataType = dataType;
    }

    public void setColumnName(String columnName)
    {
        this.columnName = columnName;
    }

    public void setLen(int len)
    {
        this.len = len;
    }

    public void setIndex(int index)
    {
        this.index = index;
    }

    public void setKey(int key)
    {
        this.key = key;
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((columnName == null) ? 0 : columnName.hashCode());
        result = prime * result + ((tableName == null) ? 0 : tableName.hashCode());
        result = prime * result + dataType;
        result = prime * result + id;
        result = prime * result + index;
        result = prime * result + key;
        result = prime * result + len;
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
        Column other = (Column) obj;
        if (columnName == null) {
            if (other.columnName != null) {
                return false;
            }
        }
        else if (!columnName.equals(other.columnName)) {
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
        if (dataType != other.dataType) {
            return false;
        }
        if (id != other.id) {
            return false;
        }
        if (index != other.index) {
            return false;
        }
        if (key != other.key) {
            return false;
        }
        if (len != other.len) {
            return false;
        }
        return true;
    }

    public String getTableName()
    {
        return tableName;
    }

    public void setTableName(String tableName)
    {
        this.tableName = tableName;
    }

    public String toString()
    {
        if (tableName == null) {
            return columnName;
        }
        else {
            return tableName + "." + columnName;
        }
    }
}
