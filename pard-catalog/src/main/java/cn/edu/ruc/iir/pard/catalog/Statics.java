package cn.edu.ruc.iir.pard.catalog;

import java.util.HashMap;

public class Statics
{
    private String columnName;
    private String min;
    private String max;
    private String mean;
    private String mode;
    private String median;
    private HashMap<String, Integer> staticList;
    public Statics()
    {
    }

    public String getColumnName()
    {
        return columnName;
    }

    public String getMin()
    {
        return min;
    }

    public String getMax()
    {
        return max;
    }

    public String getMean()
    {
        return mean;
    }

    public String getMode()
    {
        return mode;
    }

    public String getMedian()
    {
        return median;
    }

    public HashMap<String, Integer> getStaticList()
    {
        return staticList;
    }

    public void setColumnName(String columnName)
    {
        this.columnName = columnName;
    }

    public void setMin(String min)
    {
        this.min = min;
    }

    public void setMax(String max)
    {
        this.max = max;
    }

    public void setMean(String mean)
    {
        this.mean = mean;
    }

    public void setMode(String mode)
    {
        this.mode = mode;
    }

    public void setMedian(String median)
    {
        this.median = median;
    }

    public void setStaticList(HashMap<String, Integer> staticList)
    {
        this.staticList = staticList;
    }
}
