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
    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", "OUTPUT")
                .add("child", getLeftChild())
                .toString();
    }
}
