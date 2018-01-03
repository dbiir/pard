package cn.edu.ruc.iir.pard.sql.tree;

import java.nio.ByteBuffer;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * pard
 *
 * @author guodong
 */
public class StringLiteral
        extends Literal
{
    private static final long serialVersionUID = -5230281126527636285L;
    private final String value;
    private final transient ByteBuffer slice;

    public StringLiteral(String value)
    {
        this(null, value);
    }

    public StringLiteral(Location location, String value)
    {
        super(location);
        requireNonNull(value, "value is null");
        this.value = value;
        this.slice = ByteBuffer.wrap(value.getBytes());
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
        return visitor.visitStringLiteral(this, context);
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

        StringLiteral that = (StringLiteral) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode()
    {
        return value.hashCode();
    }
}
