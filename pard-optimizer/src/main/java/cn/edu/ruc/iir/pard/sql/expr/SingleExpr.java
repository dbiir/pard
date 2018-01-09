package cn.edu.ruc.iir.pard.sql.expr;

import cn.edu.ruc.iir.pard.catalog.GddUtil;
import cn.edu.ruc.iir.pard.sql.tree.ComparisonExpression;
import cn.edu.ruc.iir.pard.sql.tree.Expression;

public class SingleExpr
        extends Expr
{
    private Item lvalue;
    private Item rvalue;
    private int compareType;
    public SingleExpr(SingleExpr expr)
    {
        super();
        this.lvalue = Item.clone(expr.lvalue);
        this.rvalue = Item.clone(expr.rvalue);
        this.compareType = expr.compareType;
    }
    public SingleExpr(Item lvalue, Item rvalue, int compareType)
    {
        super();
        this.lvalue = lvalue;
        this.rvalue = rvalue;
        this.compareType = compareType;
    }
    public SingleExpr(Expression expression)
    {
        ComparisonExpression expr = (ComparisonExpression) expression;
        switch (expr.getType()) {
            case EQUAL:
                this.compareType = GddUtil.compareEQUAL;
                break;
            case GREATER_THAN:
                this.compareType = GddUtil.compareGREAT;
                break;
            case IS_DISTINCT_FROM:
                throw new NullPointerException("is distinct from not implemented!");
            case GREATER_THAN_OR_EQUAL:
                this.compareType = GddUtil.compareGREATEQUAL;
                break;
            case LESS_THAN:
                this.compareType = GddUtil.compareLESS;
                break;
            case LESS_THAN_OR_EQUAL:
                this.compareType = GddUtil.compareLESSEQUAL;
                break;
            case NOT_EQUAL:
                this.compareType = GddUtil.compareNOTEQUAL;
                break;
            default:
                break;
        }
        //TODO:
        lvalue = Item.parse(expr.getLeft());
        rvalue = Item.parse(expr.getRight());
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
    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + compareType;
        result = prime * result + ((lvalue == null) ? 0 : lvalue.hashCode());
        result = prime * result + ((rvalue == null) ? 0 : rvalue.hashCode());
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
        SingleExpr other = (SingleExpr) obj;
        if (compareType != other.compareType) {
            return false;
        }
        if (lvalue == null) {
            if (other.lvalue != null) {
                return false;
            }
        }
        else if (!lvalue.equals(other.lvalue)) {
            return false;
        }
        if (rvalue == null) {
            if (other.rvalue != null) {
                return false;
            }
        }
        else if (!rvalue.equals(other.rvalue)) {
            return false;
        }
        return true;
    }
}
