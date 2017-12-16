package cn.edu.ruc.iir.pard.catalog;

public class Condition
{
    private String columnName;
    private int compareType; //define less,great,equal..
    private String value;
    private int dataType; //the datatype of value
    public Condition()
    {
    }

    public Condition(String columnName, int compareType, String value, int dataType)
    {
        this.columnName = columnName;
        this.compareType = compareType;
        this.value = value;
        this.dataType = dataType;
    }

    public String getColumnName()
    {
        return columnName;
    }

    public int getCompareType()
    {
        return compareType;
    }

    public String getValue()
    {
        return value;
    }

    public int getDataType()
    {
        return dataType;
    }

    public void setColumnName(String columnName)
    {
        this.columnName = columnName;
    }

    public void setCompareType(int compareType)
    {
        this.compareType = compareType;
    }

    public void setValue(String value)
    {
        this.value = value;
    }

    public void setDataType(int dataType)
    {
        this.dataType = dataType;
    }
}
