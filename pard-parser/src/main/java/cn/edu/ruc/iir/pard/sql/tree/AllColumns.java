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
public class AllColumns
        extends SelectItem
{
    private static final long serialVersionUID = -4092581908128380462L;
    private final Optional<QualifiedName> prefix;

    public AllColumns()
    {
        super(null);
        prefix = Optional.empty();
    }

    public AllColumns(Location location)
    {
        super(location);
        prefix = Optional.empty();
    }

    public AllColumns(QualifiedName prefix)
    {
        this(null, prefix);
    }

    public AllColumns(Location location, QualifiedName prefix)
    {
        super(location);
        requireNonNull(prefix, "prefix is null");
        this.prefix = Optional.of(prefix);
    }

    public Optional<QualifiedName> getPrefix()
    {
        return prefix;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitAllColumns(this, context);
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

        AllColumns that = (AllColumns) o;
        return Objects.equals(prefix, that.prefix);
    }

    @Override
    public int hashCode()
    {
        return prefix.hashCode();
    }

    @Override
    public String toString()
    {
        if (prefix.isPresent()) {
            return prefix.get() + ".*";
        }

        return "*";
    }
}
