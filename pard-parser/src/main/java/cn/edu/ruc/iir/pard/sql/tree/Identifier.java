package cn.edu.ruc.iir.pard.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * pard
 *
 * @author guodong
 */
public final class Identifier
        extends Expression
{
    private static final Pattern NAME_PATTERN = Pattern.compile("[a-zA-Z_]([a-zA-Z0-9_:@])*");
    private static final long serialVersionUID = 7099655458789595639L;

    private final String value;
    private final boolean delimited;

    public Identifier(Location location, String value, boolean delimited)
    {
        super(location);
        this.value = value;
        this.delimited = delimited;

        checkArgument(delimited || NAME_PATTERN.matcher(value).matches(), "Value contains illegal characters: %s", value);
    }

    public Identifier(String value, boolean delimited)
    {
        this(null, value, delimited);
    }

    public Identifier(String value)
    {
        this(null, value, !NAME_PATTERN.matcher(value).matches());
    }

    public String getValue()
    {
        return value;
    }

    public boolean isDelimited()
    {
        return delimited;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitIdentifier(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of();
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

        Identifier that = (Identifier) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode()
    {
        return value.hashCode();
    }
}
