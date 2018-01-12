package cn.edu.ruc.iir.pard.executor.connector.node;

import static com.google.common.base.MoreObjects.toStringHelper;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * pard
 *
 * @author guodong
 */
public class JoinNode
        extends CartesianNode
{
    private static final long serialVersionUID = 3355047142533066940L;
    private Set<String> joinSet;
    public JoinNode()
    {
        name = "JOIN";
        joinSet = new HashSet<String>();
    }
    public JoinNode(JoinNode node)
    {
        super(node);
        name = "JOIN";
        joinSet = new HashSet<String>();
        joinSet.addAll(node.joinSet);
    }
    public boolean hasChildren()
    {
        return !childrens.isEmpty();
    }
    public void addUnionChild(PlanNode node)
    {
        this.childrens.add(node);
    }

    public List<PlanNode> getJoinChildren()
    {
        return childrens;
    }
    public Set<String> getJoinSet()
    {
        return joinSet;
    }
    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", "JOIN")
                .add("children", childrens)
                .add("joinSet", this.joinSet)
                .toString();
    }
}
