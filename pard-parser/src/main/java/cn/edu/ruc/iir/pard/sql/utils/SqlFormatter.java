package cn.edu.ruc.iir.pard.sql.utils;

import cn.edu.ruc.iir.pard.sql.tree.AliasedRelation;
import cn.edu.ruc.iir.pard.sql.tree.AllColumns;
import cn.edu.ruc.iir.pard.sql.tree.AstVisitor;
import cn.edu.ruc.iir.pard.sql.tree.CreateSchema;
import cn.edu.ruc.iir.pard.sql.tree.DropSchema;
import cn.edu.ruc.iir.pard.sql.tree.DropTable;
import cn.edu.ruc.iir.pard.sql.tree.Except;
import cn.edu.ruc.iir.pard.sql.tree.Expression;
import cn.edu.ruc.iir.pard.sql.tree.Identifier;
import cn.edu.ruc.iir.pard.sql.tree.Intersect;
import cn.edu.ruc.iir.pard.sql.tree.Join;
import cn.edu.ruc.iir.pard.sql.tree.JoinCriteria;
import cn.edu.ruc.iir.pard.sql.tree.JoinOn;
import cn.edu.ruc.iir.pard.sql.tree.JoinUsing;
import cn.edu.ruc.iir.pard.sql.tree.NaturalJoin;
import cn.edu.ruc.iir.pard.sql.tree.Node;
import cn.edu.ruc.iir.pard.sql.tree.OrderBy;
import cn.edu.ruc.iir.pard.sql.tree.QualifiedName;
import cn.edu.ruc.iir.pard.sql.tree.QuerySpecification;
import cn.edu.ruc.iir.pard.sql.tree.Relation;
import cn.edu.ruc.iir.pard.sql.tree.Row;
import cn.edu.ruc.iir.pard.sql.tree.SampledRelation;
import cn.edu.ruc.iir.pard.sql.tree.Select;
import cn.edu.ruc.iir.pard.sql.tree.SelectItem;
import cn.edu.ruc.iir.pard.sql.tree.SingleColumn;
import cn.edu.ruc.iir.pard.sql.tree.Table;
import cn.edu.ruc.iir.pard.sql.tree.TableSubquery;
import cn.edu.ruc.iir.pard.sql.tree.Union;
import cn.edu.ruc.iir.pard.sql.tree.Values;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static cn.edu.ruc.iir.pard.sql.utils.ExpressionFormatter.formatExpression;
import static cn.edu.ruc.iir.pard.sql.utils.ExpressionFormatter.formatGroupBy;
import static cn.edu.ruc.iir.pard.sql.utils.ExpressionFormatter.formatOrderBy;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.stream.Collectors.joining;

/**
 * pard
 *
 * @author guodong
 */

public final class SqlFormatter
{
    private static final String INDENT = "   ";
    private static final Pattern NAME_PATTERN = Pattern.compile("[a-z_][a-z0-9_]*");

    private SqlFormatter() {}

    public static String formatSql(Node root, Optional<List<Expression>> parameters)
    {
        StringBuilder builder = new StringBuilder();
        new Formatter(builder, parameters).process(root, 0);
        return builder.toString();
    }

    private static class Formatter
            extends AstVisitor<Void, Integer>
    {
        private final StringBuilder builder;
        private final Optional<List<Expression>> parameters;

        public Formatter(StringBuilder builder, Optional<List<Expression>> parameters)
        {
            this.builder = builder;
            this.parameters = parameters;
        }

        @Override
        protected Void visitNode(Node node, Integer indent)
        {
            throw new UnsupportedOperationException("not yet implemented: " + node);
        }

        @Override
        protected Void visitExpression(Expression node, Integer indent)
        {
            checkArgument(indent == 0, "visitExpression should only be called at root");
            builder.append(formatExpression(node, parameters));
            return null;
        }

        @Override
        protected Void visitQuerySpecification(QuerySpecification node, Integer indent)
        {
            process(node.getSelect(), indent);

            if (node.getFrom().isPresent()) {
                append(indent, "FROM");
                builder.append('\n');
                append(indent, "  ");
                process(node.getFrom().get(), indent);
            }

            builder.append('\n');

            if (node.getWhere().isPresent()) {
                append(indent, "WHERE " + formatExpression(node.getWhere().get(), parameters))
                        .append('\n');
            }

            if (node.getGroupBy().isPresent()) {
                append(indent, "GROUP BY " + (node.getGroupBy().get().isDistinct() ? " DISTINCT " : "") + formatGroupBy(node.getGroupBy().get().getGroupingElements())).append('\n');
            }

            if (node.getHaving().isPresent()) {
                append(indent, "HAVING " + formatExpression(node.getHaving().get(), parameters))
                        .append('\n');
            }

            if (node.getOrderBy().isPresent()) {
                process(node.getOrderBy().get(), indent);
            }

            if (node.getLimit().isPresent()) {
                append(indent, "LIMIT " + node.getLimit().get())
                        .append('\n');
            }
            return null;
        }

        @Override
        protected Void visitOrderBy(OrderBy node, Integer indent)
        {
            append(indent, formatOrderBy(node, parameters))
                    .append('\n');
            return null;
        }

        @Override
        protected Void visitSelect(Select node, Integer indent)
        {
            append(indent, "SELECT");
            if (node.isDistinct()) {
                builder.append(" DISTINCT");
            }

            if (node.getSelectItems().size() > 1) {
                boolean first = true;
                for (SelectItem item : node.getSelectItems()) {
                    builder.append("\n")
                            .append(indentString(indent))
                            .append(first ? "  " : ", ");

                    process(item, indent);
                    first = false;
                }
            }
            else {
                builder.append(' ');
                process(getOnlyElement(node.getSelectItems()), indent);
            }

            builder.append('\n');

            return null;
        }

        @Override
        protected Void visitSingleColumn(SingleColumn node, Integer indent)
        {
            builder.append(formatExpression(node.getExpression(), parameters));
            if (node.getAlias().isPresent()) {
                builder.append(' ')
                        .append(formatExpression(node.getAlias().get(), parameters));
            }

            return null;
        }

        @Override
        protected Void visitAllColumns(AllColumns node, Integer context)
        {
            builder.append(node.toString());

            return null;
        }

        @Override
        protected Void visitTable(Table node, Integer indent)
        {
            builder.append(formatName(node.getName()));

            return null;
        }

        @Override
        protected Void visitJoin(Join node, Integer indent)
        {
            JoinCriteria criteria = node.getCriteria().orElse(null);
            String type = node.getType().toString();
            if (criteria instanceof NaturalJoin) {
                type = "NATURAL " + type;
            }

            if (node.getType() != Join.Type.IMPLICIT) {
                builder.append('(');
            }
            process(node.getLeft(), indent);

            builder.append('\n');
            if (node.getType() == Join.Type.IMPLICIT) {
                append(indent, ", ");
            }
            else {
                append(indent, type).append(" JOIN ");
            }

            process(node.getRight(), indent);

            if (node.getType() != Join.Type.CROSS && node.getType() != Join.Type.IMPLICIT) {
                if (criteria instanceof JoinUsing) {
                    JoinUsing using = (JoinUsing) criteria;
                    builder.append(" USING (")
                            .append(Joiner.on(", ").join(using.getColumns()))
                            .append(")");
                }
                else if (criteria instanceof JoinOn) {
                    JoinOn on = (JoinOn) criteria;
                    builder.append(" ON ")
                            .append(formatExpression(on.getExpression(), parameters));
                }
                else if (!(criteria instanceof NaturalJoin)) {
                    throw new UnsupportedOperationException("unknown join criteria: " + criteria);
                }
            }

            if (node.getType() != Join.Type.IMPLICIT) {
                builder.append(")");
            }

            return null;
        }

        @Override
        protected Void visitAliasedRelation(AliasedRelation node, Integer indent)
        {
            process(node.getRelation(), indent);

            builder.append(' ')
                    .append(formatExpression(node.getAlias(), parameters));
            appendAliasColumns(builder, node.getColumnNames());

            return null;
        }

        @Override
        protected Void visitSampledRelation(SampledRelation node, Integer indent)
        {
            process(node.getRelation(), indent);

            builder.append(" TABLESAMPLE ")
                    .append(node.getType())
                    .append(" (")
                    .append(node.getSamplePercentage())
                    .append(')');

            return null;
        }

        @Override
        protected Void visitValues(Values node, Integer indent)
        {
            builder.append(" VALUES ");

            boolean first = true;
            for (Expression row : node.getRows()) {
                builder.append("\n")
                        .append(indentString(indent))
                        .append(first ? "  " : ", ");

                builder.append(formatExpression(row, parameters));
                first = false;
            }
            builder.append('\n');

            return null;
        }

        @Override
        protected Void visitTableSubquery(TableSubquery node, Integer indent)
        {
            builder.append('(')
                    .append('\n');

            process(node.getQuery(), indent + 1);

            append(indent, ") ");

            return null;
        }

        @Override
        protected Void visitUnion(Union node, Integer indent)
        {
            Iterator<Relation> relations = node.getRelations().iterator();

            while (relations.hasNext()) {
                processRelation(relations.next(), indent);

                if (relations.hasNext()) {
                    builder.append("UNION ");
                    if (!node.isDistinct()) {
                        builder.append("ALL ");
                    }
                }
            }

            return null;
        }

        @Override
        protected Void visitExcept(Except node, Integer indent)
        {
            processRelation(node.getLeft(), indent);

            builder.append("EXCEPT ");
            if (!node.isDistinct()) {
                builder.append("ALL ");
            }

            processRelation(node.getRight(), indent);

            return null;
        }

        @Override
        protected Void visitIntersect(Intersect node, Integer indent)
        {
            Iterator<Relation> relations = node.getRelations().iterator();

            while (relations.hasNext()) {
                processRelation(relations.next(), indent);

                if (relations.hasNext()) {
                    builder.append("INTERSECT ");
                    if (!node.isDistinct()) {
                        builder.append("ALL ");
                    }
                }
            }

            return null;
        }

        @Override
        protected Void visitCreateSchema(CreateSchema node, Integer context)
        {
            builder.append("CREATE SCHEMA ");
            if (node.isNotExists()) {
                builder.append("IF NOT EXISTS ");
            }
            builder.append(formatName(node.getSchemaName()));

            return null;
        }

        @Override
        protected Void visitDropSchema(DropSchema node, Integer context)
        {
            builder.append("DROP SCHEMA ");
            if (node.isExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(formatName(node.getSchemaName()))
                    .append(" ")
                    .append(node.isCascade() ? "CASCADE" : "RESTRICT");

            return null;
        }

        private static String formatName(String name)
        {
            if (NAME_PATTERN.matcher(name).matches()) {
                return name;
            }
            return "\"" + name.replace("\"", "\"\"") + "\"";
        }

        private static String formatName(QualifiedName name)
        {
            return name.getOriginalParts().stream()
                    .map(Formatter::formatName)
                    .collect(joining("."));
        }

        @Override
        protected Void visitDropTable(DropTable node, Integer context)
        {
            builder.append("DROP TABLE ");
            if (node.isExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(node.getTableName());

            return null;
        }

        @Override
        protected Void visitRow(Row node, Integer indent)
        {
            builder.append("ROW(");
            boolean firstItem = true;
            for (Expression item : node.getItems()) {
                if (!firstItem) {
                    builder.append(", ");
                }
                process(item, indent);
                firstItem = false;
            }
            builder.append(")");
            return null;
        }

        private void processRelation(Relation relation, Integer indent)
        {
            // TODO: handle this properly
            if (relation instanceof Table) {
                builder.append("TABLE ")
                        .append(((Table) relation).getName())
                        .append('\n');
            }
            else {
                process(relation, indent);
            }
        }

        private StringBuilder append(int indent, String value)
        {
            return builder.append(indentString(indent))
                    .append(value);
        }

        private static String indentString(int indent)
        {
            return Strings.repeat(INDENT, indent);
        }
    }

    private static void appendAliasColumns(StringBuilder builder, List<Identifier> columns)
    {
        if ((columns != null) && (!columns.isEmpty())) {
            String formattedColumns = columns.stream()
                    .map(name -> formatExpression(name, Optional.empty()))
                    .collect(Collectors.joining(", "));

            builder.append(" (")
                    .append(formattedColumns)
                    .append(')');
        }
    }
}
