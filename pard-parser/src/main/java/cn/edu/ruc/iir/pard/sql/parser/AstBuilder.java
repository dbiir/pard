package cn.edu.ruc.iir.pard.sql.parser;

import cn.edu.ruc.iir.pard.commons.exception.ParsingException;
import cn.edu.ruc.iir.pard.sql.tree.AliasedRelation;
import cn.edu.ruc.iir.pard.sql.tree.AllColumns;
import cn.edu.ruc.iir.pard.sql.tree.BinaryLiteral;
import cn.edu.ruc.iir.pard.sql.tree.BooleanLiteral;
import cn.edu.ruc.iir.pard.sql.tree.CharLiteral;
import cn.edu.ruc.iir.pard.sql.tree.ColumnDefinition;
import cn.edu.ruc.iir.pard.sql.tree.ComparisonExpression;
import cn.edu.ruc.iir.pard.sql.tree.ComparisonExpressionType;
import cn.edu.ruc.iir.pard.sql.tree.CreateIndex;
import cn.edu.ruc.iir.pard.sql.tree.CreateSchema;
import cn.edu.ruc.iir.pard.sql.tree.CreateTable;
import cn.edu.ruc.iir.pard.sql.tree.Delete;
import cn.edu.ruc.iir.pard.sql.tree.DereferenceExpression;
import cn.edu.ruc.iir.pard.sql.tree.DoubleLiteral;
import cn.edu.ruc.iir.pard.sql.tree.DropIndex;
import cn.edu.ruc.iir.pard.sql.tree.DropSchema;
import cn.edu.ruc.iir.pard.sql.tree.DropTable;
import cn.edu.ruc.iir.pard.sql.tree.Except;
import cn.edu.ruc.iir.pard.sql.tree.Expression;
import cn.edu.ruc.iir.pard.sql.tree.GenericLiteral;
import cn.edu.ruc.iir.pard.sql.tree.GroupBy;
import cn.edu.ruc.iir.pard.sql.tree.GroupingElement;
import cn.edu.ruc.iir.pard.sql.tree.Identifier;
import cn.edu.ruc.iir.pard.sql.tree.InListExpression;
import cn.edu.ruc.iir.pard.sql.tree.InPredicate;
import cn.edu.ruc.iir.pard.sql.tree.Insert;
import cn.edu.ruc.iir.pard.sql.tree.Intersect;
import cn.edu.ruc.iir.pard.sql.tree.IsNotNullPredicate;
import cn.edu.ruc.iir.pard.sql.tree.IsNullPredicate;
import cn.edu.ruc.iir.pard.sql.tree.Join;
import cn.edu.ruc.iir.pard.sql.tree.JoinCriteria;
import cn.edu.ruc.iir.pard.sql.tree.JoinOn;
import cn.edu.ruc.iir.pard.sql.tree.JoinUsing;
import cn.edu.ruc.iir.pard.sql.tree.ListPartitionElement;
import cn.edu.ruc.iir.pard.sql.tree.ListPartitionElementCondition;
import cn.edu.ruc.iir.pard.sql.tree.Location;
import cn.edu.ruc.iir.pard.sql.tree.LogicalBinaryExpression;
import cn.edu.ruc.iir.pard.sql.tree.LongLiteral;
import cn.edu.ruc.iir.pard.sql.tree.NaturalJoin;
import cn.edu.ruc.iir.pard.sql.tree.Node;
import cn.edu.ruc.iir.pard.sql.tree.NotExpression;
import cn.edu.ruc.iir.pard.sql.tree.NullLiteral;
import cn.edu.ruc.iir.pard.sql.tree.OrderBy;
import cn.edu.ruc.iir.pard.sql.tree.QualifiedName;
import cn.edu.ruc.iir.pard.sql.tree.QuantifiedComparisonExpression;
import cn.edu.ruc.iir.pard.sql.tree.Query;
import cn.edu.ruc.iir.pard.sql.tree.QueryBody;
import cn.edu.ruc.iir.pard.sql.tree.QuerySpecification;
import cn.edu.ruc.iir.pard.sql.tree.RangePartitionElement;
import cn.edu.ruc.iir.pard.sql.tree.RangePartitionElementCondition;
import cn.edu.ruc.iir.pard.sql.tree.Relation;
import cn.edu.ruc.iir.pard.sql.tree.Row;
import cn.edu.ruc.iir.pard.sql.tree.SampledRelation;
import cn.edu.ruc.iir.pard.sql.tree.Select;
import cn.edu.ruc.iir.pard.sql.tree.SelectItem;
import cn.edu.ruc.iir.pard.sql.tree.ShowSchemas;
import cn.edu.ruc.iir.pard.sql.tree.ShowTables;
import cn.edu.ruc.iir.pard.sql.tree.SingleColumn;
import cn.edu.ruc.iir.pard.sql.tree.SortItem;
import cn.edu.ruc.iir.pard.sql.tree.StringLiteral;
import cn.edu.ruc.iir.pard.sql.tree.SubqueryExpression;
import cn.edu.ruc.iir.pard.sql.tree.Table;
import cn.edu.ruc.iir.pard.sql.tree.TableElement;
import cn.edu.ruc.iir.pard.sql.tree.TableHHashPartitioner;
import cn.edu.ruc.iir.pard.sql.tree.TableHListPartitioner;
import cn.edu.ruc.iir.pard.sql.tree.TableHPartitioner;
import cn.edu.ruc.iir.pard.sql.tree.TableHRangePartitioner;
import cn.edu.ruc.iir.pard.sql.tree.TableSubquery;
import cn.edu.ruc.iir.pard.sql.tree.TableVPartitioner;
import cn.edu.ruc.iir.pard.sql.tree.Union;
import cn.edu.ruc.iir.pard.sql.tree.Use;
import cn.edu.ruc.iir.pard.sql.tree.Values;
import com.google.common.collect.ImmutableList;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.lang.String.format;
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
        return new Insert(
                getLocation(ctx),
                getQualifiedName(ctx.qualifiedName()),
                Optional.ofNullable(getColumnAliases(ctx.columnAliases())),
                (Query) visit(ctx.query()));
    }

    @Override
    public Node visitDelete(PardSqlBaseParser.DeleteContext ctx)
    {
        return new Delete(
                getLocation(ctx),
                getQualifiedName(ctx.qualifiedName()),
                (Expression) visit(ctx.booleanExpression()));
    }

    @Override
    public Node visitShowSchemas(PardSqlBaseParser.ShowSchemasContext ctx)
    {
        return new ShowSchemas(getLocation(ctx));
    }

    @Override
    public Node visitShowTables(PardSqlBaseParser.ShowTablesContext ctx)
    {
        if (ctx.FROM() != null) {
            return new ShowTables(getLocation(ctx),
                    (Identifier) visit(ctx.schemaName));
        }
        return new ShowTables(getLocation(ctx));
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
        return new RangePartitionElement(
                getLocation(ctx),
                (Identifier) visit(ctx.partitionName),
                visit(ctx.rangePartitionElementCon(), RangePartitionElementCondition.class),
                ctx.node == null ? null : ((Identifier) visit(ctx.node)).toString());
    }

    @Override
    public Node visitRangePartitionElementCon(PardSqlBaseParser.RangePartitionElementConContext ctx)
    {
        RangePartitionElementCondition.Predicate predicate = RangePartitionElementCondition.Predicate.NULL;
        if (ctx.partitionPre.EQ() != null) {
            predicate = RangePartitionElementCondition.Predicate.EQUAL;
        }
        if (ctx.partitionPre.LT() != null) {
            predicate = RangePartitionElementCondition.Predicate.LESS;
        }
        if (ctx.partitionPre.GT() != null) {
            predicate = RangePartitionElementCondition.Predicate.GREATER;
        }
        if (ctx.partitionPre.LTE() != null) {
            predicate = RangePartitionElementCondition.Predicate.LESSEQ;
        }
        if (ctx.partitionPre.GTE() != null) {
            predicate = RangePartitionElementCondition.Predicate.GREATEREQ;
        }

        return new RangePartitionElementCondition(
                getLocation(ctx),
                (Identifier) visit(ctx.partitionCol),
                predicate,
                ctx.partitionExpr == null ? null : (Expression) visit(ctx.partitionExpr),
                ctx.MINVALUE() != null,
                ctx.MAXVALUE() != null);
    }

    @Override
    public Node visitListPartitionElement(PardSqlBaseParser.ListPartitionElementContext ctx)
    {
        return new ListPartitionElement(
                getLocation(ctx),
                (Identifier) visit(ctx.partitionName),
                visit(ctx.listPartitionElementCon(), ListPartitionElementCondition.class),
                ctx.node == null ? null : ((Identifier) visit(ctx.node)).toString());
    }

    @Override
    public Node visitListPartitionElementCon(PardSqlBaseParser.ListPartitionElementConContext ctx)
    {
        return new ListPartitionElementCondition(
                getLocation(ctx),
                (Identifier) visit(ctx.partitionCol),
                visit(ctx.expression(), Expression.class));
    }

    @Override
    public Node visitColumnDefinition(PardSqlBaseParser.ColumnDefinitionContext ctx)
    {
        return new ColumnDefinition(
                getLocation(ctx),
                (Identifier) visit(ctx.identifier()),
                getType(ctx.type()),
                ctx.PRIMARY() != null);
    }

    @Override
    public Node visitQuery(PardSqlBaseParser.QueryContext ctx)
    {
        Optional<String> limitOption = getTextIfPresent(ctx.limit);

        QueryBody term = (QueryBody) visit(ctx.queryTerm());

        Optional<OrderBy> orderBy = Optional.empty();
        if (ctx.ORDER() != null) {
            orderBy = Optional.of(new OrderBy(getLocation(ctx.ORDER()), visit(ctx.sortItem(), SortItem.class)));
        }

        if (term instanceof QuerySpecification) {
            QuerySpecification query = (QuerySpecification) term;

            return new Query(
                    getLocation(ctx),
                    new QuerySpecification(
                            getLocation(ctx),
                            query.getSelect(),
                            query.getFrom(),
                            query.getWhere(),
                            query.getGroupBy(),
                            query.getHaving(),
                            orderBy,
                            getTextIfPresent(ctx.limit)),
                    null,
                    null);
        }

        return new Query(
                getLocation(ctx),
                term,
                orderBy.orElse(null),
                getTextIfPresent(ctx.limit).isPresent() ? getTextIfPresent(ctx.limit).get() : null);
    }

    @Override
    public Node visitSetOperation(PardSqlBaseParser.SetOperationContext ctx)
    {
        QueryBody left = (QueryBody) visit(ctx.left);
        QueryBody right = (QueryBody) visit(ctx.right);

        boolean distinct = ctx.setQuantifier() == null || ctx.setQuantifier().DISTINCT() != null;

        switch (ctx.operator.getType()) {
            case PardSqlBaseLexer.UNION:
                return new Union(getLocation(ctx.UNION()), ImmutableList.of(left, right), distinct);
            case PardSqlBaseLexer.INTERSECT:
                return new Intersect(getLocation(ctx.INTERSECT()), ImmutableList.of(left, right), distinct);
            case PardSqlBaseLexer.EXCEPT:
                return new Except(getLocation(ctx.EXCEPT()), left, right, distinct);
        }

        throw new IllegalArgumentException("Unsupported set operation: " + ctx.operator.getText());
    }

    @Override
    public Node visitTable(PardSqlBaseParser.TableContext ctx)
    {
        return new Table(
                getLocation(ctx),
                getQualifiedName(ctx.qualifiedName()));
    }

    @Override
    public Node visitInlineTable(PardSqlBaseParser.InlineTableContext ctx)
    {
        return new Values(
                getLocation(ctx),
                visit(ctx.expression(), Expression.class));
    }

    @Override
    public Node visitSubquery(PardSqlBaseParser.SubqueryContext ctx)
    {
        return new TableSubquery(
                getLocation(ctx),
                (Query) visit(ctx.query()));
    }

    @Override
    public Node visitSortItem(PardSqlBaseParser.SortItemContext ctx)
    {
        return new SortItem(
                getLocation(ctx),
                (Expression) visit(ctx.expression()),
                Optional.ofNullable(ctx.ordering)
                .map(AstBuilder::getOrderingType)
                .orElse(SortItem.Ordering.ASCENDING),
                Optional.ofNullable(ctx.nullOrdering)
                .map(AstBuilder::getNullOrderingType)
                .orElse(SortItem.NullOrdering.UNDEFINED));
    }

    @Override
    public Node visitQuerySpecification(PardSqlBaseParser.QuerySpecificationContext ctx)
    {
        Optional<Relation> from = Optional.empty();
        List<SelectItem> selectItems = visit(ctx.selectItem(), SelectItem.class);
        List<Relation> relations = visit(ctx.relation(), Relation.class);
        if (!relations.isEmpty()) {
            Iterator<Relation> iterator = relations.iterator();
            Relation relation = iterator.next();

            while (iterator.hasNext()) {
                relation = new Join(
                        getLocation(ctx),
                        Join.Type.IMPLICIT,
                        relation,
                        iterator.next(),
                        Optional.empty());
            }

            from = Optional.of(relation);
        }

        return new QuerySpecification(
                getLocation(ctx),
                new Select(getLocation(ctx.SELECT()),
                        isDistinct(ctx.setQuantifier()), selectItems),
                from,
                visitIfPresent(ctx.where, Expression.class),
                visitIfPresent(ctx.groupBy(), GroupBy.class),
                visitIfPresent(ctx.having, Expression.class),
                Optional.empty(),
                Optional.empty());
    }

    @Override
    public Node visitGroupBy(PardSqlBaseParser.GroupByContext ctx)
    {
        return new GroupBy(
                getLocation(ctx),
                isDistinct(ctx.setQuantifier()),
                visit(ctx.groupingElement(), GroupingElement.class));
    }

    @Override
    public Node visitSingleGroupingSet(PardSqlBaseParser.SingleGroupingSetContext ctx)
    {
        return new GroupingElement(
                getLocation(ctx),
                visit(ctx.groupingExpressions().expression(), Expression.class));
    }

    @Override
    public Node visitSelectSingle(PardSqlBaseParser.SelectSingleContext ctx)
    {
        return new SingleColumn(
                getLocation(ctx),
                (Expression) visit(ctx.expression()),
                visitIfPresent(ctx.identifier(), Identifier.class));
    }

    @Override
    public Node visitSelectAll(PardSqlBaseParser.SelectAllContext ctx)
    {
        if (ctx.qualifiedName() != null) {
            return new AllColumns(
                    getLocation(ctx),
                    getQualifiedName(ctx.qualifiedName()));
        }

        return new AllColumns(getLocation(ctx));
    }

    @Override
    public Node visitJoinRelation(PardSqlBaseParser.JoinRelationContext ctx)
    {
        Relation left = (Relation) visit(ctx.left);
        Relation right;

        if (ctx.CROSS() != null) {
            right = (Relation) visit(ctx.right);
            return new Join(getLocation(ctx), Join.Type.CROSS, left, right, Optional.empty());
        }

        JoinCriteria criteria;
        if (ctx.NATURAL() != null) {
            right = (Relation) visit(ctx.right);
            criteria = new NaturalJoin();
        }
        else {
            right = (Relation) visit(ctx.rightRelation);
            if (ctx.joinCriteria().ON() != null) {
                criteria = new JoinOn((Expression) visit(ctx.joinCriteria().booleanExpression()));
            }
            else if (ctx.joinCriteria().USING() != null) {
                criteria = new JoinUsing(visit(ctx.joinCriteria().identifier(), Identifier.class));
            }
            else {
                throw new IllegalArgumentException("Unsupported join criteria");
            }
        }

        Join.Type joinType;
        if (ctx.joinType().LEFT() != null) {
            joinType = Join.Type.LEFT;
        }
        else if (ctx.joinType().RIGHT() != null) {
            joinType = Join.Type.RIGHT;
        }
        else if (ctx.joinType().FULL() != null) {
            joinType = Join.Type.FULL;
        }
        else {
            joinType = Join.Type.INNER;
        }

        return new Join(
                getLocation(ctx),
                joinType,
                left,
                right,
                Optional.of(criteria));
    }

    @Override
    public Node visitSampledRelation(PardSqlBaseParser.SampledRelationContext ctx)
    {
        Relation child = (Relation) visit(ctx.aliasedRelation());

        if (ctx.TABLESAMPLE() == null) {
            return child;
        }

        return new SampledRelation(
                getLocation(ctx),
                child,
                getSamplingMethod((Token) ctx.sampleType().getChild(0).getPayload()),
                (Expression) visit(ctx.percentage));
    }

    @Override
    public Node visitAliasedRelation(PardSqlBaseParser.AliasedRelationContext ctx)
    {
        Relation child = (Relation) visit(ctx.relationPrimary());

        if (ctx.identifier() == null) {
            return child;
        }

        List<Identifier> aliases = null;
        if (ctx.columnAliases() != null) {
            aliases = visit(ctx.columnAliases().identifier(), Identifier.class);
        }

        return new AliasedRelation(
                getLocation(ctx),
                child,
                (Identifier) visit(ctx.identifier()),
                aliases);
    }

    @Override
    public Node visitTableName(PardSqlBaseParser.TableNameContext ctx)
    {
        return new Table(
                getLocation(ctx),
                getQualifiedName(ctx.qualifiedName()));
    }

    @Override
    public Node visitSubqueryRelation(PardSqlBaseParser.SubqueryRelationContext ctx)
    {
        return new TableSubquery(
                getLocation(ctx),
                (Query) visit(ctx.query()));
    }

    @Override
    public Node visitParenthesizedRelation(PardSqlBaseParser.ParenthesizedRelationContext ctx)
    {
        return visit(ctx.relation());
    }

    @Override
    public Node visitLogicalNot(PardSqlBaseParser.LogicalNotContext ctx)
    {
        return new NotExpression(
                getLocation(ctx),
                (Expression) visit(ctx.booleanExpression()));
    }

    @Override
    public Node visitLogicalBinary(PardSqlBaseParser.LogicalBinaryContext ctx)
    {
        return new LogicalBinaryExpression(
                getLocation(ctx),
                getLogicalBinaryOperator(ctx.operator),
                (Expression) visit(ctx.left),
                (Expression) visit(ctx.right));
    }

    @Override
    public Node visitPredicated(PardSqlBaseParser.PredicatedContext ctx)
    {
        if (ctx.predicate() != null) {
            return visit(ctx.predicate());
        }

        return visit(ctx.valueExpression);
    }

    @Override
    public Node visitComparison(PardSqlBaseParser.ComparisonContext ctx)
    {
        return new ComparisonExpression(
                getLocation(ctx),
                getComparisonOperator(((TerminalNode) ctx.comparisonOperator().getChild(0)).getSymbol()),
                (Expression) visit(ctx.value),
                (Expression) visit(ctx.right));
    }

    @Override
    public Node visitQuantifiedComparison(PardSqlBaseParser.QuantifiedComparisonContext ctx)
    {
        return new QuantifiedComparisonExpression(
                getLocation(ctx),
                getComparisonOperator(((TerminalNode) ctx.comparisonOperator().getChild(0)).getSymbol()),
                getComparisonQuantifier(((TerminalNode) ctx.comparisonQuantifier().getChild(0)).getSymbol()),
                (Expression) visit(ctx.value),
                new SubqueryExpression(
                        getLocation(ctx),
                        (Query) visit(ctx.query())));
    }

    @Override
    public Node visitInList(PardSqlBaseParser.InListContext ctx)
    {
        Expression result = new InPredicate(
                getLocation(ctx),
                (Expression) visit(ctx.value),
                new InListExpression(getLocation(ctx), visit(ctx.expression(), Expression.class)));

        if (ctx.NOT() != null) {
            result = new NotExpression(getLocation(ctx), result);
        }

        return result;
    }

    @Override
    public Node visitInSubquery(PardSqlBaseParser.InSubqueryContext ctx)
    {
        Expression result = new InPredicate(
                getLocation(ctx),
                (Expression) visit(ctx.value),
                new SubqueryExpression(getLocation(ctx), (Query) visit(ctx.query())));

        if (ctx.NOT() != null) {
            result = new NotExpression(getLocation(ctx), result);
        }

        return result;
    }

    @Override
    public Node visitNullPredicate(PardSqlBaseParser.NullPredicateContext ctx)
    {
        Expression child = (Expression) visit(ctx.value);

        if (ctx.NOT() == null) {
            return new IsNullPredicate(getLocation(ctx), child);
        }

        return new IsNotNullPredicate(getLocation(ctx), child);
    }

    @Override
    public Node visitArithmeticBinary(PardSqlBaseParser.ArithmeticBinaryContext ctx)
    {
        // todo arithmetic binary
        return null;
    }

    @Override
    public Node visitArithmeticUnary(PardSqlBaseParser.ArithmeticUnaryContext ctx)
    {
        // todo arithmetic unary
        return null;
    }

    @Override
    public Node visitDereference(PardSqlBaseParser.DereferenceContext ctx)
    {
        return new DereferenceExpression(
                getLocation(ctx),
                (Expression) visit(ctx.base),
                (Identifier) visit(ctx.fieldName));
    }

    @Override
    public Node visitTypeConstructor(PardSqlBaseParser.TypeConstructorContext ctx)
    {
        String value = ((StringLiteral) visit(ctx.string())).getValue();

        if (ctx.DOUBLE_PRECISION() != null) {
            // TODO: Temporary hack that should be removed with new planner.
            return new DoubleLiteral(getLocation(ctx), value);
        }

        String type = ctx.identifier().getText();
        if (type.equalsIgnoreCase("char")) {
            return new CharLiteral(getLocation(ctx), value);
        }
        if (type.equalsIgnoreCase("int") || type.equalsIgnoreCase("long")) {
            return new LongLiteral(getLocation(ctx), value);
        }

        return new GenericLiteral(getLocation(ctx), type, value);
    }

    @Override
    public Node visitParenthesizedExpression(PardSqlBaseParser.ParenthesizedExpressionContext ctx)
    {
        return visit(ctx.expression());
    }

    @Override
    public Node visitColumnReference(PardSqlBaseParser.ColumnReferenceContext ctx)
    {
        return visit(ctx.identifier());
    }

    @Override
    public Node visitNullLiteral(PardSqlBaseParser.NullLiteralContext ctx)
    {
        return new NullLiteral(
                getLocation(ctx));
    }

    @Override
    public Node visitRowConstructor(PardSqlBaseParser.RowConstructorContext ctx)
    {
        return new Row(getLocation(ctx), visit(ctx.expression(), Expression.class));
    }

    @Override
    public Node visitSubqueryExpression(PardSqlBaseParser.SubqueryExpressionContext ctx)
    {
        return new SubqueryExpression(
                getLocation(ctx),
                (Query) visit(ctx.query()));
    }

    @Override
    public Node visitBinaryLiteral(PardSqlBaseParser.BinaryLiteralContext ctx)
    {
        return new BinaryLiteral(
                getLocation(ctx),
                ctx.getText());
    }

    @Override
    public Node visitStringLiteral(PardSqlBaseParser.StringLiteralContext ctx)
    {
        return new StringLiteral(
                getLocation(ctx),
                ctx.getText());
    }

    @Override
    public Node visitBasicStringLiteral(PardSqlBaseParser.BasicStringLiteralContext ctx)
    {
        return new StringLiteral(
                getLocation(ctx),
                unquote(ctx.getText()));
    }

    @Override
    public Node visitUnicodeStringLiteral(PardSqlBaseParser.UnicodeStringLiteralContext ctx)
    {
        return new StringLiteral(
                getLocation(ctx),
                decodeUnicodeLiteral(ctx));
    }

    @Override
    public Node visitBooleanValue(PardSqlBaseParser.BooleanValueContext ctx)
    {
        return new BooleanLiteral(
                getLocation(ctx),
                ctx.getText());
    }

    @Override
    public Node visitFilter(PardSqlBaseParser.FilterContext ctx)
    {
        return visit(ctx.booleanExpression());
    }

    @Override
    public Node visitReadUncommitted(PardSqlBaseParser.ReadUncommittedContext ctx)
    {
        // todo read uncommitted
        return null;
    }

    @Override
    public Node visitReadCommitted(PardSqlBaseParser.ReadCommittedContext ctx)
    {
        // todo read committed
        return null;
    }

    @Override
    public Node visitRepeatableRead(PardSqlBaseParser.RepeatableReadContext ctx)
    {
        // todo repeatable read
        return null;
    }

    @Override
    public Node visitSerializable(PardSqlBaseParser.SerializableContext ctx)
    {
        // todo serializable
        return null;
    }

    @Override
    public Node visitUnquotedIdentifier(PardSqlBaseParser.UnquotedIdentifierContext ctx)
    {
        return new Identifier(
                getLocation(ctx),
                ctx.getText(),
                false);
    }

    @Override
    public Node visitQuotedIdentifier(PardSqlBaseParser.QuotedIdentifierContext ctx)
    {
        String token = ctx.getText();
        String identifier = token.substring(1, token.length() - 1)
                .replace("\"\"", "\"");

        return new Identifier(getLocation(ctx), identifier, true);
    }

    @Override
    public Node visitDecimalLiteral(PardSqlBaseParser.DecimalLiteralContext ctx)
    {
        return new DoubleLiteral(
                getLocation(ctx),
                ctx.getText());
    }

    @Override
    public Node visitIntegerLiteral(PardSqlBaseParser.IntegerLiteralContext ctx)
    {
        return new LongLiteral(
                getLocation(ctx),
                ctx.getText());
    }

    private <T> Optional<T> visitIfPresent(ParserRuleContext context, Class<T> clazz)
    {
        return Optional.ofNullable(context)
                .map(this::visit)
                .map(clazz::cast);
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

    private static Optional<String> getTextIfPresent(Token token)
    {
        return Optional.ofNullable(token)
                .map(Token::getText);
    }

    private String getType(PardSqlBaseParser.TypeContext type)
    {
        if (type.baseType() != null) {
            String signature = type.baseType().getText();
            if (type.baseType().DOUBLE_PRECISION() != null) {
                // TODO: Temporary hack that should be removed with new planner.
                signature = "DOUBLE";
            }
            if (!type.typeParameter().isEmpty()) {
                String typeParameterSignature = type
                        .typeParameter()
                        .stream()
                        .map(this::typeParameterToString)
                        .collect(Collectors.joining(","));
                signature += "(" + typeParameterSignature + ")";
            }
            return signature;
        }

        if (type.ARRAY() != null) {
            return "ARRAY(" + getType(type.type(0)) + ")";
        }

        if (type.MAP() != null) {
            return "MAP(" + getType(type.type(0)) + "," + getType(type.type(1)) + ")";
        }

        if (type.ROW() != null) {
            StringBuilder builder = new StringBuilder("(");
            for (int i = 0; i < type.identifier().size(); i++) {
                if (i != 0) {
                    builder.append(",");
                }
                builder.append(visit(type.identifier(i)))
                        .append(" ")
                        .append(getType(type.type(i)));
            }
            builder.append(")");
            return "ROW" + builder.toString();
        }

        throw new IllegalArgumentException("Unsupported type specification: " + type.getText());
    }

    private static List<String> getColumnAliases(PardSqlBaseParser.ColumnAliasesContext columnAliasesContext)
    {
        if (columnAliasesContext == null) {
            return null;
        }

        return columnAliasesContext
                .identifier().stream()
                .map(ParseTree::getText)
                .collect(toList());
    }

    private String typeParameterToString(PardSqlBaseParser.TypeParameterContext typeParameter)
    {
        if (typeParameter.INTEGER_VALUE() != null) {
            return typeParameter.INTEGER_VALUE().toString();
        }
        if (typeParameter.type() != null) {
            return getType(typeParameter.type());
        }
        throw new IllegalArgumentException("Unsupported typeParameter: " + typeParameter.getText());
    }

    private static SortItem.Ordering getOrderingType(Token token)
    {
        switch (token.getType()) {
            case PardSqlBaseLexer.ASC:
                return SortItem.Ordering.ASCENDING;
            case PardSqlBaseLexer.DESC:
                return SortItem.Ordering.DESCENDING;
        }

        throw new IllegalArgumentException("Unsupported ordering: " + token.getText());
    }

    private static SortItem.NullOrdering getNullOrderingType(Token token)
    {
        switch (token.getType()) {
            case PardSqlBaseLexer.FIRST:
                return SortItem.NullOrdering.FIRST;
            case PardSqlBaseLexer.LAST:
                return SortItem.NullOrdering.LAST;
        }

        throw new IllegalArgumentException("Unsupported ordering: " + token.getText());
    }

    private static boolean isDistinct(PardSqlBaseParser.SetQuantifierContext setQuantifier)
    {
        return setQuantifier != null && setQuantifier.DISTINCT() != null;
    }

    private static SampledRelation.Type getSamplingMethod(Token token)
    {
        switch (token.getType()) {
            case PardSqlBaseLexer.BERNOULLI:
                return SampledRelation.Type.USER;
            case PardSqlBaseLexer.SYSTEM:
                return SampledRelation.Type.SYSTEM;
        }

        throw new IllegalArgumentException("Unsupported sampling method: " + token.getText());
    }

    private static LogicalBinaryExpression.Type getLogicalBinaryOperator(Token token)
    {
        switch (token.getType()) {
            case PardSqlBaseLexer.AND:
                return LogicalBinaryExpression.Type.AND;
            case PardSqlBaseLexer.OR:
                return LogicalBinaryExpression.Type.OR;
        }

        throw new IllegalArgumentException("Unsupported operator: " + token.getText());
    }

    private static ComparisonExpressionType getComparisonOperator(Token symbol)
    {
        switch (symbol.getType()) {
            case PardSqlBaseLexer.EQ:
                return ComparisonExpressionType.EQUAL;
            case PardSqlBaseLexer.NEQ:
                return ComparisonExpressionType.NOT_EQUAL;
            case PardSqlBaseLexer.LT:
                return ComparisonExpressionType.LESS_THAN;
            case PardSqlBaseLexer.LTE:
                return ComparisonExpressionType.LESS_THAN_OR_EQUAL;
            case PardSqlBaseLexer.GT:
                return ComparisonExpressionType.GREATER_THAN;
            case PardSqlBaseLexer.GTE:
                return ComparisonExpressionType.GREATER_THAN_OR_EQUAL;
        }

        throw new IllegalArgumentException("Unsupported operator: " + symbol.getText());
    }

    private static QuantifiedComparisonExpression.Quantifier getComparisonQuantifier(Token symbol)
    {
        switch (symbol.getType()) {
            case PardSqlBaseLexer.ALL:
                return QuantifiedComparisonExpression.Quantifier.ALL;
            case PardSqlBaseLexer.ANY:
                return QuantifiedComparisonExpression.Quantifier.ANY;
            case PardSqlBaseLexer.SOME:
                return QuantifiedComparisonExpression.Quantifier.SOME;
        }

        throw new IllegalArgumentException("Unsupported quantifier: " + symbol.getText());
    }

    private static String unquote(String value)
    {
        return value.substring(1, value.length() - 1)
                .replace("''", "'");
    }

    private enum UnicodeDecodeState
    {
        EMPTY,
        ESCAPED,
        UNICODE_SEQUENCE
    }

    private static String decodeUnicodeLiteral(PardSqlBaseParser.UnicodeStringLiteralContext context)
    {
        char escape;
        if (context.UESCAPE() != null) {
            String escapeString = unquote(context.STRING().getText());
            check(!escapeString.isEmpty(), "Empty Unicode escape character", context);
            check(escapeString.length() == 1, "Invalid Unicode escape character: " + escapeString, context);
            escape = escapeString.charAt(0);
            check(isValidUnicodeEscape(escape), "Invalid Unicode escape character: " + escapeString, context);
        }
        else {
            escape = '\\';
        }

        String rawContent = unquote(context.UNICODE_STRING().getText().substring(2));
        StringBuilder unicodeStringBuilder = new StringBuilder();
        StringBuilder escapedCharacterBuilder = new StringBuilder();
        int charactersNeeded = 0;
        UnicodeDecodeState state = UnicodeDecodeState.EMPTY;
        for (int i = 0; i < rawContent.length(); i++) {
            char ch = rawContent.charAt(i);
            switch (state) {
                case EMPTY:
                    if (ch == escape) {
                        state = UnicodeDecodeState.ESCAPED;
                    }
                    else {
                        unicodeStringBuilder.append(ch);
                    }
                    break;
                case ESCAPED:
                    if (ch == escape) {
                        unicodeStringBuilder.append(escape);
                        state = UnicodeDecodeState.EMPTY;
                    }
                    else if (ch == '+') {
                        state = UnicodeDecodeState.UNICODE_SEQUENCE;
                        charactersNeeded = 6;
                    }
                    else if (isHexDigit(ch)) {
                        state = UnicodeDecodeState.UNICODE_SEQUENCE;
                        charactersNeeded = 4;
                        escapedCharacterBuilder.append(ch);
                    }
                    else {
                        throw parseError("Invalid hexadecimal digit: " + ch, context);
                    }
                    break;
                case UNICODE_SEQUENCE:
                    check(isHexDigit(ch), "Incomplete escape sequence: " + escapedCharacterBuilder.toString(), context);
                    escapedCharacterBuilder.append(ch);
                    if (charactersNeeded == escapedCharacterBuilder.length()) {
                        String currentEscapedCode = escapedCharacterBuilder.toString();
                        escapedCharacterBuilder.setLength(0);
                        int codePoint = Integer.parseInt(currentEscapedCode, 16);
                        check(Character.isValidCodePoint(codePoint), "Invalid escaped character: " + currentEscapedCode, context);
                        if (Character.isSupplementaryCodePoint(codePoint)) {
                            unicodeStringBuilder.appendCodePoint(codePoint);
                        }
                        else {
                            char currentCodePoint = (char) codePoint;
                            check(!Character.isSurrogate(currentCodePoint), format("Invalid escaped character: %s. Escaped character is a surrogate. Use '\\+123456' instead.", currentEscapedCode), context);
                            unicodeStringBuilder.append(currentCodePoint);
                        }
                        state = UnicodeDecodeState.EMPTY;
                        charactersNeeded = -1;
                    }
                    else {
                        check(charactersNeeded > escapedCharacterBuilder.length(), "Unexpected escape sequence length: " + escapedCharacterBuilder.length(), context);
                    }
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
        }

        check(state == UnicodeDecodeState.EMPTY, "Incomplete escape sequence: " + escapedCharacterBuilder.toString(), context);
        return unicodeStringBuilder.toString();
    }

    private static boolean isHexDigit(char c)
    {
        return ((c >= '0') && (c <= '9')) ||
                ((c >= 'A') && (c <= 'F')) ||
                ((c >= 'a') && (c <= 'f'));
    }

    private static boolean isValidUnicodeEscape(char c)
    {
        return c < 0x7F && c > 0x20 && !isHexDigit(c) && c != '"' && c != '+' && c != '\'';
    }

    private static void check(boolean condition, String message, ParserRuleContext context)
    {
        if (!condition) {
            throw parseError(message, context);
        }
    }

    private static ParsingException parseError(String message, ParserRuleContext context)
    {
        return new ParsingException(message, null, context.getStart().getLine(), context.getStart().getCharPositionInLine());
    }
}
