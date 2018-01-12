package cn.edu.ruc.iir.pard.executor.connector.node;

import cn.edu.ruc.iir.pard.catalog.Column;

import java.util.ArrayList;
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
    private static final long serialVersionUID = -1910741908996171116L;
    private final List<Column> columns;

    public DistinctNode(List<Column> columns)
    {
        this.name = "DISTINCT";
        this.columns = columns;
    }
    public DistinctNode(DistinctNode node)
    {
        super(node);
        this.name = "DISTINCT";
        this.columns = new ArrayList<Column>();
        this.columns.addAll(node.columns);
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
