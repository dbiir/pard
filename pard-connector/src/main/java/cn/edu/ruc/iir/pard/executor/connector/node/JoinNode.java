package cn.edu.ruc.iir.pard.executor.connector.node;

import cn.edu.ruc.iir.pard.sql.tree.LogicalBinaryExpression;

import static com.google.common.base.MoreObjects.toStringHelper;

import java.util.ArrayList;
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
    private List<LogicalBinaryExpression> exprList;
    public JoinNode()
    {
        name = "JOIN";
        joinSet = new HashSet<String>();
        exprList = new ArrayList<LogicalBinaryExpression>();
    }
    public JoinNode(JoinNode node)
    {
        super(node);
        name = "JOIN";
        joinSet = new HashSet<String>();
        joinSet.addAll(node.joinSet);
        exprList = new ArrayList<LogicalBinaryExpression>();
    }
    public boolean hasChildren()
    {
        return !childrens.isEmpty();
    }
    public void addJoinChild(PlanNode node)
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
    public List<LogicalBinaryExpression> getExprList()
    {
        return exprList;
    }
}
