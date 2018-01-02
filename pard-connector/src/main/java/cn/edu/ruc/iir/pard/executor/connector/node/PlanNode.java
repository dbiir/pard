package cn.edu.ruc.iir.pard.executor.connector.node;

import static com.google.common.base.MoreObjects.toStringHelper;

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

    public void setChildren(PlanNode planNode, boolean left, boolean exists)
    {
        if (exists) {
            if (childrenNum >= 2) {
                return;
            }
            childrenNum++;
        }
        if (left) {
            children[0] = planNode;
        }
        else {
            children[1] = planNode;
        }
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("left", children[0])
                .add("right", children[1])
                .toString();
    }
}
