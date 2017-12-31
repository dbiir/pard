package cn.edu.ruc.iir.pard.sql.expr;

public class ValueItem
        extends Item implements Comparable<ValueItem>
{
    @SuppressWarnings("rawtypes")
    private final Comparable comp;
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
}
