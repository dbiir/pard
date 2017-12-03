package cn.edu.ruc.iir.pard.sql.tree;

import com.google.common.base.Preconditions;

import java.util.Objects;

import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

/**
 * pard
 *
 * @author guodong
 */
public class BooleanLiteral
        extends Literal
{
    public static final BooleanLiteral TRUE_LITERAL = new BooleanLiteral(null, "true");
    public static final BooleanLiteral FALSE_LITERAL = new BooleanLiteral(null, "false");

    private final boolean value;

    public BooleanLiteral(String value)
    {
        this(null, value);
    }

    public BooleanLiteral(Location location, String value)
    {
        super(location);
        requireNonNull(value, "value is null");
        Preconditions.checkArgument(value.toLowerCase(ENGLISH).equals("true") || value.toLowerCase(ENGLISH).equals("false"));

        this.value = value.toLowerCase(ENGLISH).equals("true");
    }

    public boolean getValue()
    {
        return value;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitBooleanLiteral(this, context);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(value);
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
        BooleanLiteral other = (BooleanLiteral) obj;
        return Objects.equals(this.value, other.value);
    }
}
