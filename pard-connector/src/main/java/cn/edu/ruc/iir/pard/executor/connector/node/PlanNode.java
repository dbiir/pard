package cn.edu.ruc.iir.pard.executor.connector.node;

/**
 * pard
 *
 * @author guodong
 */
public abstract class PlanNode
{
    private int childrenNum = 0;
    private PlanNode[] children = new PlanNode[2];

    public boolean hasChildren()
    {
        return childrenNum > 0;
    }

    public PlanNode getLeftChild()
    {
        return children[0];
    }

    public PlanNode getRightChild()
    {
        return children[1];
    }

    public void setChildren(PlanNode planNode, boolean left)
    {
        if (left) {
            children[0] = planNode;
        }
        else {
            children[1] = planNode;
        }
        childrenNum++;
    }
}
