package cn.edu.ruc.iir.pard.sql.tree;

import java.util.List;
import java.util.Optional;

/**
 * pard
 *
 * @author guodong
 */
public abstract class Node
{
    private final Location location;

    protected Node(Location location)
    {
        this.location = location;
    }

    protected <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitNode(this, context);
    }

    public abstract List<? extends Node> getChildren();

    public Optional<Location> getLocation()
    {
        if (location == null) {
            return Optional.empty();
        }
        return Optional.of(location);
    }

    @Override
    public abstract int hashCode();

    @Override
    public abstract boolean equals(Object obj);

    @Override
    public abstract String toString();
}
