package cn.edu.ruc.iir.pard.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * pard
 *
 * @author guodong
 */
public class CreateTable
        extends Statement
{
    private final QualifiedName name;
    private final List<TableVPartition> verticalPartitions;
    private final boolean notExists;
    private final Optional<TableHPartitioner> horizontalPartitioner;

    public CreateTable(QualifiedName name, List<TableVPartition> verticalPartitions, boolean notExists)
    {
        this(null, name, verticalPartitions, notExists, Optional.empty());
    }

    public CreateTable(Location location, QualifiedName name, List<TableVPartition> verticalPartitions, boolean notExists, Optional<TableHPartitioner> horizontalPartition)
    {
        super(location);
        this.name = requireNonNull(name, "table is null");
        this.verticalPartitions = ImmutableList.copyOf(requireNonNull(verticalPartitions, "elements is null"));
        this.notExists = notExists;
        this.horizontalPartitioner = horizontalPartition;
    }

    public QualifiedName getName()
    {
        return name;
    }

    public List<TableVPartition> getVerticalPartitions()
    {
        return verticalPartitions;
    }

    public boolean isNotExists()
    {
        return notExists;
    }

    public Optional<TableHPartitioner> getHorizontalPartition()
    {
        return horizontalPartitioner;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitCreateTable(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        ImmutableList.Builder<Node> nodes = ImmutableList.builder();
        nodes.addAll(verticalPartitions);
        horizontalPartitioner.ifPresent(nodes::add);
        return nodes.build();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, horizontalPartitioner, notExists, horizontalPartitioner);
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
        CreateTable o = (CreateTable) obj;
        return Objects.equals(name, o.name) &&
                Objects.equals(verticalPartitions, o.verticalPartitions) &&
                Objects.equals(notExists, o.notExists) &&
                Objects.equals(horizontalPartitioner, o.horizontalPartitioner);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("vertical partitions", verticalPartitions)
                .add("notExists", notExists)
                .add("horizontal partitions", horizontalPartitioner.orElse(null))
                .omitNullValues()
                .toString();
    }
}