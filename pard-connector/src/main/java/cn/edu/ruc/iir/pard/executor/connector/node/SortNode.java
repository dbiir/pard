package cn.edu.ruc.iir.pard.executor.connector.node;

import cn.edu.ruc.iir.pard.catalog.Column;

import java.util.ArrayList;
import java.util.List;

/**
 * pard
 *
 * @author guodong
 */
public class SortNode
        extends PlanNode
{
    private final List<Column> columns;
    private final List<Integer> orderings;

    public SortNode()
    {
        this.columns = new ArrayList<>();
        this.orderings = new ArrayList<>();
    }

    public void addSort(Column column, boolean order)
    {
        if (!columns.contains(column)) {
            this.columns.add(column);
            this.orderings.add(order ? 1 : 0);
        }
    }
}
