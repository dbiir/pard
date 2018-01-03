package cn.edu.ruc.iir.pard.executor.connector.node;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * pard
 *
 * @author guodong
 */
public class LimitNode
        extends PlanNode
{
    private static final long serialVersionUID = -8191276433605066712L;
    private final int limitNum;

    public LimitNode(int limitNum)
    {
        this.limitNum = limitNum;
    }

    public int getLimitNum()
    {
        return limitNum;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", "LIMIT")
                .add("number", limitNum)
                .add("child", getLeftChild())
                .toString();
    }
}
