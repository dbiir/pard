package cn.edu.ruc.iir.pard.sql.tree;

import javax.annotation.Nullable;
/**
 * pard
 *
 * @author guodong
 */
public abstract class AstVisitor<R, C>
{
    public R process(Node node)
    {
        return process(node, null);
    }

    public R process(Node node, @Nullable C context)
    {
        return node.accept(this, context);
    }

    protected R visitNode(Node node, C context)
    {
        return null;
    }

    protected R visitStatement(Statement node, C context)
    {
        return visitNode(node, context);
    }

    protected R visitUse(Use node, C context)
    {
        return visitStatement(node, context);
    }

    protected R visitExpression(Expression node, C context)
    {
        return visitNode(node, context);
    }

    protected R visitIdentifier(Identifier node, C context)
    {
        return visitExpression(node, context);
    }
}
