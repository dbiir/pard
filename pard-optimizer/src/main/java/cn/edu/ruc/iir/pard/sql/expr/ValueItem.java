package cn.edu.ruc.iir.pard.sql.expr;

import cn.edu.ruc.iir.pard.sql.tree.Expression;
/**
 * ValueItem
 *
 * ValueItem class can denote a value in the expression.
 *
 * @author hagen
 * */
public class ValueItem
        extends Item implements Comparable<ValueItem>
{
    /**
     *
     */
    private static final long serialVersionUID = 2927976547609885069L;
    @SuppressWarnings("rawtypes")
    private final Comparable comp;
    public ValueItem(ValueItem vi)
    {
        super();
        this.comp = vi.comp;
        this.expression = vi.expression;
    }
    public ValueItem(@SuppressWarnings("rawtypes") Comparable comp)
    {
        super();
        this.comp = comp;
    }
    public boolean biggerThan(ValueItem value)
    {
        return this.compareTo(value) > 0;
    }
    public boolean smallerThan(ValueItem value)
    {
        return this.compareTo(value) < 0;
    }
    public boolean sameTypeAs(ValueItem value)
    {
        return comp.getClass().equals(value.getClass());
    }
    @SuppressWarnings("unchecked")
    @Override
    public int compareTo(ValueItem o)
    {
        return comp.compareTo(o.comp);
    }
    @Override
    public String toString()
    {
        return comp.toString();
    }
    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((comp == null) ? 0 : comp.hashCode());
        return result;
    }
    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ValueItem other = (ValueItem) obj;
        if (comp == null) {
            if (other.comp != null) {
                return false;
            }
        }
        else if (!comp.equals(other.comp)) {
            return false;
        }
        return true;
    }
    @Override
    public Expression toExpression()
    {
        //return null; //new DereferenceExpression(base, new Identifier(this.columnName));
        if (expression != null) {
            return expression;
        }
        else {
            return Item.parseFromComparable(comp);
        }
    }
}
