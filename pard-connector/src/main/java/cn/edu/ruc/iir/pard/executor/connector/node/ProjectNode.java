package cn.edu.ruc.iir.pard.executor.connector.node;

import cn.edu.ruc.iir.pard.catalog.Column;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * pard
 *
 * @author guodong
 */
public class ProjectNode
        extends PlanNode
{
    private static final long serialVersionUID = -6614201377588559291L;
    private final List<Column> columns;

    public ProjectNode(List<Column> columns)
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
                .add("name", "PROJECT")
                .add("columns", columns)
                .add("child", getLeftChild())
                .toString();
    }
}
