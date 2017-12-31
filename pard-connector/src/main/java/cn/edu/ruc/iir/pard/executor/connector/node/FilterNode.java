package cn.edu.ruc.iir.pard.executor.connector.node;

import cn.edu.ruc.iir.pard.sql.tree.Expression;

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
}
