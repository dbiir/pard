package cn.edu.ruc.iir.pard.sql.tree;

import cn.edu.ruc.iir.pard.commons.exception.ParsingException;

import static java.util.Objects.requireNonNull;

/**
 * pard
 *
 * @author guodong
 */
public class LongLiteral
        extends Literal
{
    private final long value;

    public LongLiteral(String value)
    {
        this(null, value);
    }

    public LongLiteral(Location location, String value)
    {
        super(location);
        requireNonNull(value, "value is null");
        try {
            this.value = Long.parseLong(value);
        }
        catch (NumberFormatException e) {
            throw new ParsingException("Invalid numeric literal: " + value);
        }
    }

    public long getValue()
    {
        return value;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitLongLiteral(this, context);
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

        LongLiteral that = (LongLiteral) o;

        if (value != that.value) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return (int) (value ^ (value >>> 32));
    }
}
