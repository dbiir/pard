package cn.edu.ruc.iir.pard.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * pard
 *
 * @author guodong
 */
public class SingleColumn
        extends SelectItem
{
    private final Optional<Identifier> alias;
    private final Expression expression;

    public SingleColumn(Expression expression)
    {
        this(null, expression, Optional.empty());
    }

    public SingleColumn(Expression expression, Optional<Identifier> alias)
    {
        this(null, expression, alias);
    }

    public SingleColumn(Expression expression, Identifier alias)
    {
        this(null, expression, Optional.of(alias));
    }

    public SingleColumn(Location location, Expression expression, Optional<Identifier> alias)
    {
        super(location);
        requireNonNull(expression, "expression is null");
        requireNonNull(alias, "alias is null");

        this.expression = expression;
        this.alias = alias;
    }

    public Optional<Identifier> getAlias()
    {
        return alias;
    }

    public Expression getExpression()
    {
        return expression;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        SingleColumn other = (SingleColumn) obj;
        return Objects.equals(this.alias, other.alias) && Objects.equals(this.expression, other.expression);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(alias, expression);
    }

    @Override
    public String toString()
    {
        if (alias.isPresent()) {
            return expression.toString() + " " + alias.get();
        }

        return expression.toString();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitSingleColumn(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of(expression);
    }
}
