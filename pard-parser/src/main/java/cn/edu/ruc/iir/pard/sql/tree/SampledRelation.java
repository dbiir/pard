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
public class SampledRelation
        extends Relation
{
    public enum Type
    {
        USER,
        SYSTEM
    }

    private final Relation relation;
    private final Type type;
    private final Expression samplePercentage;

    public SampledRelation(Relation relation, Type type, Expression samplePercentage)
    {
        this(null, relation, type, samplePercentage);
    }

    public SampledRelation(Location location, Relation relation, Type type, Expression samplePercentage)
    {
        super(location);
        this.relation = requireNonNull(relation, "relation is null");
        this.type = requireNonNull(type, "type is null");
        this.samplePercentage = requireNonNull(samplePercentage, "samplePercentage is null");
    }

    public Relation getRelation()
    {
        return relation;
    }

    public Type getType()
    {
        return type;
    }

    public Expression getSamplePercentage()
    {
        return samplePercentage;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitSampledRelation(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of(relation, samplePercentage);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("relation", relation)
                .add("type", type)
                .add("samplePercentage", samplePercentage)
                .toString();
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
        SampledRelation that = (SampledRelation) o;
        return Objects.equals(relation, that.relation) &&
                Objects.equals(type, that.type) &&
                Objects.equals(samplePercentage, that.samplePercentage);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(relation, type, samplePercentage);
    }
}
