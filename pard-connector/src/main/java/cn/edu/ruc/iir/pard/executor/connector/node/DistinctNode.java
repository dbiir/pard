package cn.edu.ruc.iir.pard.executor.connector.node;

import cn.edu.ruc.iir.pard.catalog.Column;

import java.util.List;

/**
 * pard
 *
 * @author guodong
 */
public class DistinctNode
        extends PlanNode
{
    private final List<Column> columns;

    public DistinctNode(List<Column> columns)
    {
        this.columns = columns;
    }

    public List<Column> getColumns()
    {
        return columns;
    }
}
