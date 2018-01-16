package cn.edu.ruc.iir.pard.executor.connector.node;

import java.io.Serializable;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * pard
 *
 * @author guodong
 */
public abstract class PlanNode
        implements Serializable
{
    private static final long serialVersionUID = -2786495926657736341L;
    private int childrenNum = 0;
    protected String name;
    private PlanNode[] children = new PlanNode[2];

    public PlanNode()
    {}

    public PlanNode(PlanNode pd)
    {
        this.children[0] = NodeHelper.copyNode(pd.children[0]);
        this.children[1] = NodeHelper.copyNode(pd.children[1]);
        childrenNum = pd.childrenNum;
    }
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
            //childrenNum++;
        }
        if (left) {
            children[0] = planNode;
        }
        else {
            children[1] = planNode;
        }
        childrenNum = (children[0] == null ? 0 : 1) + (children[1] == null ? 0 : 1);
    }

    public String getName()
    {
        return name;
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
