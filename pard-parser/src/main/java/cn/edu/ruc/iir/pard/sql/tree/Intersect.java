package cn.edu.ruc.iir.pard.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * pard
 *
 * @author guodong
 */
public class Intersect
        extends SetOperation
{
    private final List<Relation> relations;

    public Intersect(List<Relation> relations, boolean distinct)
    {
        this(null, relations, distinct);
    }

    private Intersect(Location location, List<Relation> relations, boolean distinct)
    {
        super(location, distinct);
        requireNonNull(relations, "relations is null");

        this.relations = ImmutableList.copyOf(relations);
    }

    public List<Relation> getRelations()
    {
        return relations;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitIntersect(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return relations;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("relations", relations)
                .add("distinct", isDistinct())
                .toString();
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
        Intersect o = (Intersect) obj;
        return Objects.equals(relations, o.relations) &&
                Objects.equals(isDistinct(), o.isDistinct());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(relations, isDistinct());
    }
}
