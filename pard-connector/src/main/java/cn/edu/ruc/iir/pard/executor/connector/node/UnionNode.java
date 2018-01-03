package cn.edu.ruc.iir.pard.executor.connector.node;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * pard
 *
 * @author guodong
 */
public class UnionNode
        extends PlanNode
{
    private static final long serialVersionUID = -7395583971872559217L;
    private final List<PlanNode> unionChildren;

    public UnionNode()
    {
        this.unionChildren = new ArrayList<>();
    }

    @Override
    public boolean hasChildren()
    {
        return !unionChildren.isEmpty();
    }

    public void addUnionChild(PlanNode node)
    {
        this.unionChildren.add(node);
    }

    public List<PlanNode> getUnionChildren()
    {
        return unionChildren;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", "UNION")
                .add("children", unionChildren)
                .toString();
    }
}
