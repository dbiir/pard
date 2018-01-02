package cn.edu.ruc.iir.pard.executor.connector.node;

import cn.edu.ruc.iir.pard.sql.tree.Expression;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * pard
 *
 * @author guodong
 */
public class FilterNode
        extends PlanNode
{
    private final Expression expression;

    public FilterNode(Expression expression)
    {
        this.expression = expression;
    }

    public Expression getExpression()
    {
        return expression;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", "FILTER")
                .add("expression", expression)
                .add("child", getLeftChild())
                .toString();
    }
}
