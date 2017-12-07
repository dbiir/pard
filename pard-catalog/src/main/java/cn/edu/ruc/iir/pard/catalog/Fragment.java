package cn.edu.ruc.iir.pard.catalog;

import java.util.List;

public class Fragment
{
    private int fragmentType; //0: horizontal;1:vertical
    private List<Condition> condition;
    private List<String> tableList;
    private int siteId;
    private Statics statics;
    public Fragment()
    {
    }

    public Fragment(int fragmentType, List<Condition> condition, List<String> tableList, int siteId, Statics statics)
    {
        this.fragmentType = fragmentType;
        this.condition = condition;
        this.tableList = tableList;
        this.siteId = siteId;
        this.statics = statics;
    }

    public int getFragmentType()
    {
        return fragmentType;
    }

    public List<Condition> getCondition()
    {
        return condition;
    }

    public List<String> getTableList()
    {
        return tableList;
    }

    public int getSiteId()
    {
        return siteId;
    }

    public Statics getStatics()
    {
        return statics;
    }

    public void setFragmentType(int fragmentType)
    {
        this.fragmentType = fragmentType;
    }

    public void setCondition(List<Condition> condition)
    {
        this.condition = condition;
    }

    public void setTableList(List<String> tableList)
    {
        this.tableList = tableList;
    }

    public void setSiteId(int siteId)
    {
        this.siteId = siteId;
    }

    public void setStatics(Statics statics)
    {
        this.statics = statics;
    }
}
