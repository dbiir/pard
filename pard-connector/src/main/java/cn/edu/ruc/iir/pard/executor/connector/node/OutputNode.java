package cn.edu.ruc.iir.pard.executor.connector.node;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * pard
 *
 * @author guodong
 */
public class OutputNode
        extends PlanNode
{
    private static final long serialVersionUID = 1547246938332568325L;

    public OutputNode()
    {
        name = "OutputNode";
    }
    public OutputNode(OutputNode node)
    {
        super(node);
        name = "OutputNode";
    }
    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", "OUTPUT")
                .add("child", getLeftChild())
                .toString();
    }
}
