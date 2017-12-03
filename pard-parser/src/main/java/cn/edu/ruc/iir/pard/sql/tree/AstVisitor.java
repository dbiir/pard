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

    protected R visitCreateSchema(CreateSchema node, C context)
    {
        return visitStatement(node, context);
    }

    protected R visitDropSchema(DropSchema node, C context)
    {
        return visitStatement(node, context);
    }

    protected R visitCreateTable(CreateTable node, C context)
    {
        return visitStatement(node, context);
    }

    protected R visitDropTable(DropTable node, C context)
    {
        return visitStatement(node, context);
    }

    protected R visitUse(Use node, C context)
    {
        return visitStatement(node, context);
    }

    protected R visitTableHPartitioner(TableHPartitioner node, C context)
    {
        return visitStatement(node, context);
    }

    protected R visitTableVPartitioner(TableVPartitioner node, C context)
    {
        return visitStatement(node, context);
    }

    protected R visitTableHHashPartitioner(TableHHashPartitioner node, C context)
    {
        return visitTableHPartitioner(node, context);
    }

    protected R visitTableHListPartitioner(TableHListPartitioner node, C context)
    {
        return visitTableHPartitioner(node, context);
    }

    protected R visitTableHRangePartitioner(TableHRangePartitioner node, C context)
    {
        return visitTableHPartitioner(node, context);
    }

    protected R visitExpression(Expression node, C context)
    {
        return visitNode(node, context);
    }

    protected R visitIdentifier(Identifier node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitTableElement(TableElement node, C context)
    {
        return visitNode(node, context);
    }

    protected R visitColumnDefinition(ColumnDefinition node, C context)
    {
        return visitTableElement(node, context);
    }

    protected R visitRelation(Relation node, C context)
    {
        return visitNode(node, context);
    }

    protected R visitQueryBody(QueryBody node, C context)
    {
        return visitRelation(node, context);
    }

    protected R visitSetOperation(SetOperation node, C context)
    {
        return visitQueryBody(node, context);
    }

    protected R visitQuerySpecification(QuerySpecification node, C context)
    {
        return visitQueryBody(node, context);
    }

    protected R visitTable(Table node, C context)
    {
        return visitQueryBody(node, context);
    }

    protected R visitTableSubquery(TableSubquery node, C context)
    {
        return visitQueryBody(node, context);
    }

    protected R visitValues(Values node, C context)
    {
        return visitQueryBody(node, context);
    }

    protected R visitExcept(Except node, C context)
    {
        return visitSetOperation(node, context);
    }

    protected R visitIntersect(Intersect node, C context)
    {
        return visitSetOperation(node, context);
    }

    protected R visitUnion(Union node, C context)
    {
        return visitSetOperation(node, context);
    }

    protected R visitLimit(Limit node, C context)
    {
        return visitNode(node, context);
    }

    protected R visitOrderBy(OrderBy node, C context)
    {
        return visitNode(node, context);
    }

    protected R visitSortItem(SortItem node, C context)
    {
        return visitNode(node, context);
    }
}
