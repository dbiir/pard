package cn.edu.ruc.iir.pard.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * pard
 *
 * @author guodong
 */
public class Delete
        extends Statement
{
    private static final long serialVersionUID = 2391635195460350652L;
    private final QualifiedName name;
    private final Expression expression;

    public Delete(QualifiedName name, Expression expression)
    {
        this(null, name, expression);
    }

    public Delete(Location location, QualifiedName name, Expression expression)
    {
        super(location);
        this.name = name;
        this.expression = expression;
    }

    public QualifiedName getName()
    {
        return name;
    }

    public Expression getExpression()
    {
        return expression;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitDelete(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, expression);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        Delete o = (Delete) obj;
        return Objects.equals(name, o.name) &&
                Objects.equals(expression, o.expression);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("expression", expression)
                .omitNullValues()
                .toString();
    }
}
