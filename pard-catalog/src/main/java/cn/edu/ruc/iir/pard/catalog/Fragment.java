package cn.edu.ruc.iir.pard.catalog;

import java.util.List;

public class Fragment
{
    private int fragmentType; //0: horizontal;1:vertical
    private List<Condition> condition;
    private Table subTable;
    private String siteName;
    //private Statics statics;
    private String fragmentName;
    public Fragment()
    {
    }

    public void setSiteName(String siteName)
    {
        this.siteName = siteName;
    }

    public String getSiteName()
    {
        return siteName;
    }

    public Fragment(int fragmentType, List<Condition> condition, Table subTable, String siteName, String fragmentName)
    {
        this.fragmentType = fragmentType;
        this.condition = condition;
        this.subTable = subTable;
        this.siteName = siteName;
        this.fragmentName = fragmentName;
    }

    public int getFragmentType()
    {
        return fragmentType;
    }

    public List<Condition> getCondition()
    {
        return condition;
    }

    public void setFragmentType(int fragmentType)
    {
        this.fragmentType = fragmentType;
    }

    public void setCondition(List<Condition> condition)
    {
        this.condition = condition;
    }

    public void setFragmentName(String fragmentName)
    {
        this.fragmentName = fragmentName;
    }

    public String getFragmentName()
    {
        return fragmentName;
    }

    public void setSubTable(Table subTable)
    {
        this.subTable = subTable;
    }

    public Table getSubTable()
    {
        return subTable;
    }
}
