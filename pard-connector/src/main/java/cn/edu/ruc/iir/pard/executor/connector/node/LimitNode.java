package cn.edu.ruc.iir.pard.executor.connector.node;

/**
 * pard
 *
 * @author guodong
 */
public class LimitNode
        extends PlanNode
{
    private final int limitNum;

    public LimitNode(int limitNum)
    {
        this.limitNum = limitNum;
    }

    public int getLimitNum()
    {
        return limitNum;
    }
}
