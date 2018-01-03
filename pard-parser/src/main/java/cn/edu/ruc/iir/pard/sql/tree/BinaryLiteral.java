package cn.edu.ruc.iir.pard.sql.tree;

import cn.edu.ruc.iir.pard.commons.exception.ParsingException;
import com.google.common.io.BaseEncoding;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

/**
 * pard
 *
 * @author guodong
 */
public class BinaryLiteral
        extends Literal
{
    // the grammar could possibly include whitespace in the value it passes to us
    private static final Pattern WHITESPACE_PATTERN = Pattern.compile("[ \\r\\n\\t]");
    private static final Pattern NOT_HEX_DIGIT_PATTERN = Pattern.compile(".*[^A-F0-9].*");
    private static final long serialVersionUID = -6204610275585900624L;

    private final ByteBuffer value;

    public BinaryLiteral(String value)
    {
        this(null, value);
    }

    public BinaryLiteral(Location location, String value)
    {
        super(location);
        requireNonNull(value, "value is null");
        String hexString = WHITESPACE_PATTERN.matcher(value).replaceAll("").toUpperCase();
        if (NOT_HEX_DIGIT_PATTERN.matcher(hexString).matches()) {
            throw new ParsingException("Binary literal can only contain hexadecimal digits at", location.getLine(), location.getCharPosInLine());
        }
        if (hexString.length() % 2 != 0) {
            throw new ParsingException("Binary literal must contain an even number of digits", location.getLine(), location.getCharPosInLine());
        }
        this.value = ByteBuffer.wrap(BaseEncoding.base16().decode(hexString));
    }

    /**
     * Return the valued as a hex-formatted string with upper-case characters
     */
    public String toHexString()
    {
        return BaseEncoding.base16().encode(value.array());
    }

    public ByteBuffer getValue()
    {
        return value;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitBinaryLiteral(this, context);
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

        BinaryLiteral that = (BinaryLiteral) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode()
    {
        return value.hashCode();
    }
}
