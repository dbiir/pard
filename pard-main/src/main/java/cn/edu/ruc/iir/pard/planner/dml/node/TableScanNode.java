package cn.edu.ruc.iir.pard.planner.dml.node;

import cn.edu.ruc.iir.pard.catalog.Site;
import cn.edu.ruc.iir.pard.catalog.Table;

/**
 * pard
 *
 * @author guodong
 */
public class TableScanNode
        extends InputNode
{
    private Table table = null;
    private Site site = null;
    public Table getTable()
    {
        return table;
    }
    public void setTable(Table table)
    {
        this.table = table;
    }
    public Site getSite()
    {
        return site;
    }
    public void setSite(Site site)
    {
        this.site = site;
    }
}
