package cn.edu.ruc.iir.pard.executor.connector.node;

import cn.edu.ruc.iir.pard.catalog.Column;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;

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

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", "DISTINCT")
                .add("columns", columns)
                .add("child", getLeftChild())
                .toString();
    }
}
