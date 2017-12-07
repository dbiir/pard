package cn.edu.ruc.iir.pard.catalog;

public class Column
{
    private int id;
    private int dataType;
    private String columnName;
    private int len;
    private int index; //0:none; 1:hashindex; 2:btreeindex; 3:others
    private int key;
    public Column()
    {
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
}
