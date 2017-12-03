package cn.edu.ruc.iir.pard.sql.parser;

import cn.edu.ruc.iir.pard.sql.tree.CreateIndex;
import cn.edu.ruc.iir.pard.sql.tree.CreateSchema;
import cn.edu.ruc.iir.pard.sql.tree.CreateTable;
import cn.edu.ruc.iir.pard.sql.tree.DropIndex;
import cn.edu.ruc.iir.pard.sql.tree.DropSchema;
import cn.edu.ruc.iir.pard.sql.tree.DropTable;
import cn.edu.ruc.iir.pard.sql.tree.Expression;
import cn.edu.ruc.iir.pard.sql.tree.Identifier;
import cn.edu.ruc.iir.pard.sql.tree.ListPartitionElement;
import cn.edu.ruc.iir.pard.sql.tree.Location;
import cn.edu.ruc.iir.pard.sql.tree.Node;
import cn.edu.ruc.iir.pard.sql.tree.QualifiedName;
import cn.edu.ruc.iir.pard.sql.tree.RangePartitionElement;
import cn.edu.ruc.iir.pard.sql.tree.TableElement;
import cn.edu.ruc.iir.pard.sql.tree.TableHHashPartitioner;
import cn.edu.ruc.iir.pard.sql.tree.TableHListPartitioner;
import cn.edu.ruc.iir.pard.sql.tree.TableHPartitioner;
import cn.edu.ruc.iir.pard.sql.tree.TableHRangePartitioner;
import cn.edu.ruc.iir.pard.sql.tree.TableVPartitioner;
import cn.edu.ruc.iir.pard.sql.tree.Use;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ErrorNode;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.RuleNode;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * pard
 *
 * @author guodong
 */
public class AstBuilder
        extends PardSqlBaseBaseVisitor<Node>
{
    @Override
    public Node visitSingleStatement(PardSqlBaseParser.SingleStatementContext ctx)
    {
        return visit(ctx.statement());
    }

    @Override
    public Node visitSingleExpression(PardSqlBaseParser.SingleExpressionContext ctx)
    {
        return visit(ctx.expression());
    }

    @Override
    public Node visitUse(PardSqlBaseParser.UseContext ctx)
    {
        return new Use(
                getLocation(ctx),
                (Identifier) visit(ctx.schema));
    }

    @Override
    public Node visitCreateSchema(PardSqlBaseParser.CreateSchemaContext ctx)
    {
        return new CreateSchema(
                getLocation(ctx),
                getQualifiedName(ctx.qualifiedName()),
                ctx.EXISTS() != null);
    }

    @Override
    public Node visitDropSchema(PardSqlBaseParser.DropSchemaContext ctx)
    {
        return new DropSchema(
                getLocation(ctx),
                getQualifiedName(ctx.qualifiedName()),
                ctx.EXISTS() != null,
                ctx.CASCADE() != null);
    }

    @Override
    public Node visitCreateTable(PardSqlBaseParser.CreateTableContext ctx)
    {
        Optional<TableHPartitioner> hPartitioner = Optional.empty();
        if (ctx.partitionOps() != null) {
            hPartitioner = Optional.of((TableHPartitioner) visit(ctx.partitionOps()));
        }

        List<PardSqlBaseParser.TableElementPartContext> tableElementPartContexts = new ArrayList<>();
        List<TableVPartitioner> vPartitioners = new ArrayList<>();
        if (ctx.tableElementPart() != null) {
            tableElementPartContexts = ctx.tableElementPart();
        }
        tableElementPartContexts.forEach(
                context -> vPartitioners.add(new TableVPartitioner(
                        visit(context.tableElement(), TableElement.class),
                        context.node == null ? null : ((Identifier) visit(context.node)).getValue())));

        return new CreateTable(
                getLocation(ctx),
                getQualifiedName(ctx.qualifiedName()),
                vPartitioners,
                ctx.EXISTS() != null,
                hPartitioner);
    }

    @Override
    public Node visitCreateIndex(PardSqlBaseParser.CreateIndexContext ctx)
    {
        return new CreateIndex(
                getLocation(ctx),
                (Identifier) visit(ctx.indexName),
                getQualifiedName(ctx.indexTbl),
                visit(ctx.identifier(), Identifier.class));
    }

    @Override
    public Node visitDropIndex(PardSqlBaseParser.DropIndexContext ctx)
    {
        return new DropIndex(
                getLocation(ctx),
                (Identifier) visit(ctx.indexName));
    }

    @Override
    public Node visitDropTable(PardSqlBaseParser.DropTableContext ctx)
    {
        return new DropTable(
                getLocation(ctx),
                getQualifiedName(ctx.qualifiedName()),
                ctx.EXISTS() != null);
    }

    @Override
    public Node visitInsertInto(PardSqlBaseParser.InsertIntoContext ctx)
    {
        // todo insert into
        return null;
    }

    @Override
    public Node visitDelete(PardSqlBaseParser.DeleteContext ctx)
    {
        // todo delete
        return null;
    }

    @Override
    public Node visitExplain(PardSqlBaseParser.ExplainContext ctx)
    {
        // todo explain
        return null;
    }

    @Override
    public Node visitShowStats(PardSqlBaseParser.ShowStatsContext ctx)
    {
        // todo show stats
        return null;
    }

    @Override
    public Node visitShowStatsForQuery(PardSqlBaseParser.ShowStatsForQueryContext ctx)
    {
        // todo show stats for query
        return null;
    }

    @Override
    public Node visitShowColumns(PardSqlBaseParser.ShowColumnsContext ctx)
    {
        // todo show cols
        return null;
    }

    @Override
    public Node visitStartTransaction(PardSqlBaseParser.StartTransactionContext ctx)
    {
        // todo start tx
        return null;
    }

    @Override
    public Node visitCommit(PardSqlBaseParser.CommitContext ctx)
    {
        // todo commit
        return null;
    }

    @Override
    public Node visitRollback(PardSqlBaseParser.RollbackContext ctx)
    {
        // todo rollback
        return null;
    }

    @Override
    public Node visitShowPartitions(PardSqlBaseParser.ShowPartitionsContext ctx)
    {
        // todo show parts
        return null;
    }

    @Override
    public Node visitHashPartition(PardSqlBaseParser.HashPartitionContext ctx)
    {
        return new TableHHashPartitioner(
                getLocation(ctx),
                (Expression) visit(ctx.partitionKey),
                Integer.parseInt(ctx.INTEGER_VALUE().toString()));
    }

    @Override
    public Node visitRangePartition(PardSqlBaseParser.RangePartitionContext ctx)
    {
        return new TableHRangePartitioner(
                getLocation(ctx),
                visit(ctx.rangePartitionElement(), RangePartitionElement.class));
    }

    @Override
    public Node visitListPartition(PardSqlBaseParser.ListPartitionContext ctx)
    {
        return new TableHListPartitioner(
                getLocation(ctx),
                visit(ctx.listPartitionElement(), ListPartitionElement.class));
    }

    @Override
    public Node visitRangePartitionElement(PardSqlBaseParser.RangePartitionElementContext ctx)
    {
        return null;
    }

    @Override
    public Node visitListPartitionElement(PardSqlBaseParser.ListPartitionElementContext ctx)
    {
        return null;
    }

    @Override
    public Node visitColumnDefinition(PardSqlBaseParser.ColumnDefinitionContext ctx)
    {
        return null;
    }

    @Override
    public Node visitQuery(PardSqlBaseParser.QueryContext ctx)
    {
        return null;
    }

    @Override
    public Node visitSetOperation(PardSqlBaseParser.SetOperationContext ctx)
    {
        return null;
    }

    @Override
    public Node visitTable(PardSqlBaseParser.TableContext ctx)
    {
        return null;
    }

    @Override
    public Node visitInlineTable(PardSqlBaseParser.InlineTableContext ctx)
    {
        return null;
    }

    @Override
    public Node visitSubquery(PardSqlBaseParser.SubqueryContext ctx)
    {
        return null;
    }

    @Override
    public Node visitSortItem(PardSqlBaseParser.SortItemContext ctx)
    {
        return null;
    }

    @Override
    public Node visitQuerySpecification(PardSqlBaseParser.QuerySpecificationContext ctx)
    {
        return null;
    }

    @Override
    public Node visitGroupBy(PardSqlBaseParser.GroupByContext ctx)
    {
        return null;
    }

    @Override
    public Node visitSingleGroupingSet(PardSqlBaseParser.SingleGroupingSetContext ctx)
    {
        return null;
    }

    @Override
    public Node visitGroupingExpressions(PardSqlBaseParser.GroupingExpressionsContext ctx)
    {
        return null;
    }

    @Override
    public Node visitGroupingSet(PardSqlBaseParser.GroupingSetContext ctx)
    {
        return null;
    }

    @Override
    public Node visitSetQuantifier(PardSqlBaseParser.SetQuantifierContext ctx)
    {
        return null;
    }

    @Override
    public Node visitSelectSingle(PardSqlBaseParser.SelectSingleContext ctx)
    {
        return null;
    }

    @Override
    public Node visitSelectAll(PardSqlBaseParser.SelectAllContext ctx)
    {
        return null;
    }

    @Override
    public Node visitRelationDefault(PardSqlBaseParser.RelationDefaultContext ctx)
    {
        return null;
    }

    @Override
    public Node visitJoinRelation(PardSqlBaseParser.JoinRelationContext ctx)
    {
        return null;
    }

    @Override
    public Node visitJoinType(PardSqlBaseParser.JoinTypeContext ctx)
    {
        return null;
    }

    @Override
    public Node visitJoinCriteria(PardSqlBaseParser.JoinCriteriaContext ctx)
    {
        return null;
    }

    @Override
    public Node visitSampledRelation(PardSqlBaseParser.SampledRelationContext ctx)
    {
        return null;
    }

    @Override
    public Node visitSampleType(PardSqlBaseParser.SampleTypeContext ctx)
    {
        return null;
    }

    @Override
    public Node visitAliasedRelation(PardSqlBaseParser.AliasedRelationContext ctx)
    {
        return null;
    }

    @Override
    public Node visitColumnAliases(PardSqlBaseParser.ColumnAliasesContext ctx)
    {
        return null;
    }

    @Override
    public Node visitTableName(PardSqlBaseParser.TableNameContext ctx)
    {
        return null;
    }

    @Override
    public Node visitSubqueryRelation(PardSqlBaseParser.SubqueryRelationContext ctx)
    {
        return null;
    }

    @Override
    public Node visitParenthesizedRelation(PardSqlBaseParser.ParenthesizedRelationContext ctx)
    {
        return null;
    }

    @Override
    public Node visitExpression(PardSqlBaseParser.ExpressionContext ctx)
    {
        return null;
    }

    @Override
    public Node visitLogicalNot(PardSqlBaseParser.LogicalNotContext ctx)
    {
        return null;
    }

    @Override
    public Node visitBooleanDefault(PardSqlBaseParser.BooleanDefaultContext ctx)
    {
        return null;
    }

    @Override
    public Node visitLogicalBinary(PardSqlBaseParser.LogicalBinaryContext ctx)
    {
        return null;
    }

    @Override
    public Node visitPredicated(PardSqlBaseParser.PredicatedContext ctx)
    {
        return null;
    }

    @Override
    public Node visitComparison(PardSqlBaseParser.ComparisonContext ctx)
    {
        return null;
    }

    @Override
    public Node visitQuantifiedComparison(PardSqlBaseParser.QuantifiedComparisonContext ctx)
    {
        return null;
    }

    @Override
    public Node visitInList(PardSqlBaseParser.InListContext ctx)
    {
        return null;
    }

    @Override
    public Node visitInSubquery(PardSqlBaseParser.InSubqueryContext ctx)
    {
        return null;
    }

    @Override
    public Node visitNullPredicate(PardSqlBaseParser.NullPredicateContext ctx)
    {
        return null;
    }

    @Override
    public Node visitValueExpressionDefault(PardSqlBaseParser.ValueExpressionDefaultContext ctx)
    {
        return null;
    }

    @Override
    public Node visitArithmeticBinary(PardSqlBaseParser.ArithmeticBinaryContext ctx)
    {
        return null;
    }

    @Override
    public Node visitArithmeticUnary(PardSqlBaseParser.ArithmeticUnaryContext ctx)
    {
        return null;
    }

    @Override
    public Node visitDereference(PardSqlBaseParser.DereferenceContext ctx)
    {
        return null;
    }

    @Override
    public Node visitTypeConstructor(PardSqlBaseParser.TypeConstructorContext ctx)
    {
        return null;
    }

    @Override
    public Node visitParenthesizedExpression(PardSqlBaseParser.ParenthesizedExpressionContext ctx)
    {
        return null;
    }

    /**
     * Visit a parse tree produced by the {@code intervalLiteral}
     * labeled alternative in {@link PardSqlBaseParser#primaryExpression}.
     *
     * @param ctx the parse tree
     * @return the visitor result
     */
    @Override
    public Node visitIntervalLiteral(PardSqlBaseParser.IntervalLiteralContext ctx)
    {
        return null;
    }

    /**
     * Visit a parse tree produced by the {@code numericLiteral}
     * labeled alternative in {@link PardSqlBaseParser#primaryExpression}.
     *
     * @param ctx the parse tree
     * @return the visitor result
     */
    @Override
    public Node visitNumericLiteral(PardSqlBaseParser.NumericLiteralContext ctx)
    {
        return null;
    }

    /**
     * Visit a parse tree produced by the {@code booleanLiteral}
     * labeled alternative in {@link PardSqlBaseParser#primaryExpression}.
     *
     * @param ctx the parse tree
     * @return the visitor result
     */
    @Override
    public Node visitBooleanLiteral(PardSqlBaseParser.BooleanLiteralContext ctx)
    {
        return null;
    }

    /**
     * Visit a parse tree produced by the {@code simpleCase}
     * labeled alternative in {@link PardSqlBaseParser#primaryExpression}.
     *
     * @param ctx the parse tree
     * @return the visitor result
     */
    @Override
    public Node visitSimpleCase(PardSqlBaseParser.SimpleCaseContext ctx)
    {
        return null;
    }

    /**
     * Visit a parse tree produced by the {@code columnReference}
     * labeled alternative in {@link PardSqlBaseParser#primaryExpression}.
     *
     * @param ctx the parse tree
     * @return the visitor result
     */
    @Override
    public Node visitColumnReference(PardSqlBaseParser.ColumnReferenceContext ctx)
    {
        return null;
    }

    /**
     * Visit a parse tree produced by the {@code nullLiteral}
     * labeled alternative in {@link PardSqlBaseParser#primaryExpression}.
     *
     * @param ctx the parse tree
     * @return the visitor result
     */
    @Override
    public Node visitNullLiteral(PardSqlBaseParser.NullLiteralContext ctx)
    {
        return null;
    }

    /**
     * Visit a parse tree produced by the {@code rowConstructor}
     * labeled alternative in {@link PardSqlBaseParser#primaryExpression}.
     *
     * @param ctx the parse tree
     * @return the visitor result
     */
    @Override
    public Node visitRowConstructor(PardSqlBaseParser.RowConstructorContext ctx)
    {
        return null;
    }

    /**
     * Visit a parse tree produced by the {@code subscript}
     * labeled alternative in {@link PardSqlBaseParser#primaryExpression}.
     *
     * @param ctx the parse tree
     * @return the visitor result
     */
    @Override
    public Node visitSubscript(PardSqlBaseParser.SubscriptContext ctx)
    {
        return null;
    }

    /**
     * Visit a parse tree produced by the {@code subqueryExpression}
     * labeled alternative in {@link PardSqlBaseParser#primaryExpression}.
     *
     * @param ctx the parse tree
     * @return the visitor result
     */
    @Override
    public Node visitSubqueryExpression(PardSqlBaseParser.SubqueryExpressionContext ctx)
    {
        return null;
    }

    /**
     * Visit a parse tree produced by the {@code binaryLiteral}
     * labeled alternative in {@link PardSqlBaseParser#primaryExpression}.
     *
     * @param ctx the parse tree
     * @return the visitor result
     */
    @Override
    public Node visitBinaryLiteral(PardSqlBaseParser.BinaryLiteralContext ctx)
    {
        return null;
    }

    /**
     * Visit a parse tree produced by the {@code extract}
     * labeled alternative in {@link PardSqlBaseParser#primaryExpression}.
     *
     * @param ctx the parse tree
     * @return the visitor result
     */
    @Override
    public Node visitExtract(PardSqlBaseParser.ExtractContext ctx)
    {
        return null;
    }

    /**
     * Visit a parse tree produced by the {@code stringLiteral}
     * labeled alternative in {@link PardSqlBaseParser#primaryExpression}.
     *
     * @param ctx the parse tree
     * @return the visitor result
     */
    @Override
    public Node visitStringLiteral(PardSqlBaseParser.StringLiteralContext ctx)
    {
        return null;
    }

    @Override
    public Node visitExists(PardSqlBaseParser.ExistsContext ctx)
    {
        return null;
    }

    /**
     * Visit a parse tree produced by the {@code position}
     * labeled alternative in {@link PardSqlBaseParser#primaryExpression}.
     *
     * @param ctx the parse tree
     * @return the visitor result
     */
    @Override
    public Node visitPosition(PardSqlBaseParser.PositionContext ctx)
    {
        return null;
    }

    @Override
    public Node visitGroupingOperation(PardSqlBaseParser.GroupingOperationContext ctx)
    {
        return null;
    }

    /**
     * Visit a parse tree produced by the {@code basicStringLiteral}
     * labeled alternative in {@link PardSqlBaseParser#string}.
     *
     * @param ctx the parse tree
     * @return the visitor result
     */
    @Override
    public Node visitBasicStringLiteral(PardSqlBaseParser.BasicStringLiteralContext ctx)
    {
        return null;
    }

    /**
     * Visit a parse tree produced by the {@code unicodeStringLiteral}
     * labeled alternative in {@link PardSqlBaseParser#string}.
     *
     * @param ctx the parse tree
     * @return the visitor result
     */
    @Override
    public Node visitUnicodeStringLiteral(PardSqlBaseParser.UnicodeStringLiteralContext ctx)
    {
        return null;
    }

    @Override
    public Node visitComparisonOperator(PardSqlBaseParser.ComparisonOperatorContext ctx)
    {
        return null;
    }

    /**
     * Visit a parse tree produced by {@link PardSqlBaseParser#comparisonQuantifier}.
     *
     * @param ctx the parse tree
     * @return the visitor result
     */
    @Override
    public Node visitComparisonQuantifier(PardSqlBaseParser.ComparisonQuantifierContext ctx)
    {
        return null;
    }

    /**
     * Visit a parse tree produced by {@link PardSqlBaseParser#booleanValue}.
     *
     * @param ctx the parse tree
     * @return the visitor result
     */
    @Override
    public Node visitBooleanValue(PardSqlBaseParser.BooleanValueContext ctx)
    {
        return null;
    }

    @Override
    public Node visitType(PardSqlBaseParser.TypeContext ctx)
    {
        return null;
    }

    /**
     * Visit a parse tree produced by {@link PardSqlBaseParser#typeParameter}.
     *
     * @param ctx the parse tree
     * @return the visitor result
     */
    @Override
    public Node visitTypeParameter(PardSqlBaseParser.TypeParameterContext ctx)
    {
        return null;
    }

    /**
     * Visit a parse tree produced by {@link PardSqlBaseParser#baseType}.
     *
     * @param ctx the parse tree
     * @return the visitor result
     */
    @Override
    public Node visitBaseType(PardSqlBaseParser.BaseTypeContext ctx)
    {
        return null;
    }

    @Override
    public Node visitFilter(PardSqlBaseParser.FilterContext ctx)
    {
        return null;
    }

    @Override
    public Node visitReadUncommitted(PardSqlBaseParser.ReadUncommittedContext ctx)
    {
        return null;
    }

    /**
     * Visit a parse tree produced by the {@code readCommitted}
     * labeled alternative in {@link PardSqlBaseParser#levelOfIsolation}.
     *
     * @param ctx the parse tree
     * @return the visitor result
     */
    @Override
    public Node visitReadCommitted(PardSqlBaseParser.ReadCommittedContext ctx)
    {
        return null;
    }

    /**
     * Visit a parse tree produced by the {@code repeatableRead}
     * labeled alternative in {@link PardSqlBaseParser#levelOfIsolation}.
     *
     * @param ctx the parse tree
     * @return the visitor result
     */
    @Override
    public Node visitRepeatableRead(PardSqlBaseParser.RepeatableReadContext ctx)
    {
        return null;
    }

    /**
     * Visit a parse tree produced by the {@code serializable}
     * labeled alternative in {@link PardSqlBaseParser#levelOfIsolation}.
     *
     * @param ctx the parse tree
     * @return the visitor result
     */
    @Override
    public Node visitSerializable(PardSqlBaseParser.SerializableContext ctx)
    {
        return null;
    }

    @Override
    public Node visitPrivilege(PardSqlBaseParser.PrivilegeContext ctx)
    {
        return null;
    }

    @Override
    public Node visitQualifiedName(PardSqlBaseParser.QualifiedNameContext ctx)
    {
        return null;
    }

    @Override
    public Node visitUnquotedIdentifier(PardSqlBaseParser.UnquotedIdentifierContext ctx)
    {
        return null;
    }

    @Override
    public Node visitQuotedIdentifier(PardSqlBaseParser.QuotedIdentifierContext ctx)
    {
        return null;
    }

    /**
     * Visit a parse tree produced by the {@code backQuotedIdentifier}
     * labeled alternative in {@link PardSqlBaseParser#identifier}.
     *
     * @param ctx the parse tree
     * @return the visitor result
     */
    @Override
    public Node visitBackQuotedIdentifier(PardSqlBaseParser.BackQuotedIdentifierContext ctx)
    {
        return null;
    }

    /**
     * Visit a parse tree produced by the {@code digitIdentifier}
     * labeled alternative in {@link PardSqlBaseParser#identifier}.
     *
     * @param ctx the parse tree
     * @return the visitor result
     */
    @Override
    public Node visitDigitIdentifier(PardSqlBaseParser.DigitIdentifierContext ctx)
    {
        return null;
    }

    /**
     * Visit a parse tree produced by the {@code decimalLiteral}
     * labeled alternative in {@link PardSqlBaseParser#number}.
     *
     * @param ctx the parse tree
     * @return the visitor result
     */
    @Override
    public Node visitDecimalLiteral(PardSqlBaseParser.DecimalLiteralContext ctx)
    {
        return null;
    }

    /**
     * Visit a parse tree produced by the {@code integerLiteral}
     * labeled alternative in {@link PardSqlBaseParser#number}.
     *
     * @param ctx the parse tree
     * @return the visitor result
     */
    @Override
    public Node visitIntegerLiteral(PardSqlBaseParser.IntegerLiteralContext ctx)
    {
        return null;
    }

    /**
     * Visit a parse tree produced by {@link PardSqlBaseParser#nonReserved}.
     *
     * @param ctx the parse tree
     * @return the visitor result
     */
    @Override
    public Node visitNonReserved(PardSqlBaseParser.NonReservedContext ctx)
    {
        return null;
    }

    @Override
    public Node visit(ParseTree parseTree)
    {
        return null;
    }

    private <T> Optional<T> visitIfPresent(ParserRuleContext context, Class<T> clazz)
    {
        return Optional.ofNullable(context)
                .map(this::visit)
                .map(clazz::cast);
    }

    @Override
    public Node visitChildren(RuleNode ruleNode)
    {
        return null;
    }

    @Override
    public Node visitTerminal(TerminalNode terminalNode)
    {
        return null;
    }

    @Override
    public Node visitErrorNode(ErrorNode errorNode)
    {
        return null;
    }

    private QualifiedName getQualifiedName(PardSqlBaseParser.QualifiedNameContext context)
    {
        List<String> parts = visit(context.identifier(), Identifier.class).stream()
                .map(Identifier::getValue)
                .collect(Collectors.toList());

        return QualifiedName.of(parts);
    }

    private <T> List<T> visit(List<? extends ParserRuleContext> contexts, Class<T> clazz)
    {
        return contexts.stream()
                .map(this::visit)
                .map(clazz::cast)
                .collect(toList());
    }

    public static Location getLocation(TerminalNode terminalNode)
    {
        requireNonNull(terminalNode, "terminalNode is null");
        return getLocation(terminalNode.getSymbol());
    }

    public static Location getLocation(ParserRuleContext parserRuleContext)
    {
        requireNonNull(parserRuleContext, "parserRuleContext is null");
        return getLocation(parserRuleContext.getStart());
    }

    private static Location getLocation(Token token)
    {
        requireNonNull(token, "token is null");
        return new Location(token.getLine(), token.getCharPositionInLine());
    }
}
