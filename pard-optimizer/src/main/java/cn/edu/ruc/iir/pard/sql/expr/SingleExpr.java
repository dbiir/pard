package cn.edu.ruc.iir.pard.sql.expr;

import cn.edu.ruc.iir.pard.catalog.GddUtil;

public class SingleExpr
        extends Expr
{
    private final Item lvalue;
    private final Item rvalue;
    private final int compareType;

    public SingleExpr(Item lvalue, Item rvalue, int compareType)
    {
        super();
        this.lvalue = lvalue;
        this.rvalue = rvalue;
        this.compareType = compareType;
    }
    public int getCompareType()
    {
        return compareType;
    }
    public Item getLvalue()
    {
        return lvalue;
    }
    public Item getRvalue()
    {
        return rvalue;
    }
    @Override
    public String toString()
    {
        return lvalue.toString() + " " + GddUtil.cmpInt2Str(compareType) + " " + rvalue.toString();
    }
}
