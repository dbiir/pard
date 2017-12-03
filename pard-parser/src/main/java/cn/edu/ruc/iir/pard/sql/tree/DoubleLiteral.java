package cn.edu.ruc.iir.pard.sql.tree;

import static java.util.Objects.requireNonNull;

/**
 * pard
 *
 * @author guodong
 */
public class DoubleLiteral
        extends Literal
{
    private final double value;

    public DoubleLiteral(String value)
    {
        this(null, value);
    }

    public DoubleLiteral(Location location, String value)
    {
        super(location);
        requireNonNull(value, "value is null");
        this.value = Double.parseDouble(value);
    }

    public double getValue()
    {
        return value;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitDoubleLiteral(this, context);
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

        DoubleLiteral that = (DoubleLiteral) o;

        if (Double.compare(that.value, value) != 0) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        long temp = value != +0.0d ? Double.doubleToLongBits(value) : 0L;
        return (int) (temp ^ (temp >>> 32));
    }
}
