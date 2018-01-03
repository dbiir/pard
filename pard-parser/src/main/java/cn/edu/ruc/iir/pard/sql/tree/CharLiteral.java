package cn.edu.ruc.iir.pard.sql.tree;

import com.google.common.base.CharMatcher;

import java.nio.ByteBuffer;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * pard
 *
 * @author guodong
 */
public class CharLiteral
        extends Literal
{
    private static final long serialVersionUID = -3045374829540174578L;
    private final String value;
    private final ByteBuffer slice;

    public CharLiteral(String value)
    {
        this(null, value);
    }

    public CharLiteral(Location location, String value)
    {
        super(location);
        requireNonNull(value, "value is null");
        this.value = value;
        this.slice = ByteBuffer.wrap(CharMatcher.is(' ').trimTrailingFrom(value).getBytes());
    }

    public String getValue()
    {
        return value;
    }

    public ByteBuffer getSlice()
    {
        return slice;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitCharLiteral(this, context);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CharLiteral that = (CharLiteral) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(value);
    }
}
