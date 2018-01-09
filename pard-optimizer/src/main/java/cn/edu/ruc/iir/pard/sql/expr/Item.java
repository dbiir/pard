package cn.edu.ruc.iir.pard.sql.expr;

import cn.edu.ruc.iir.pard.sql.tree.BooleanLiteral;
import cn.edu.ruc.iir.pard.sql.tree.CharLiteral;
import cn.edu.ruc.iir.pard.sql.tree.DereferenceExpression;
import cn.edu.ruc.iir.pard.sql.tree.DoubleLiteral;
import cn.edu.ruc.iir.pard.sql.tree.Expression;
import cn.edu.ruc.iir.pard.sql.tree.Literal;
import cn.edu.ruc.iir.pard.sql.tree.LongLiteral;
import cn.edu.ruc.iir.pard.sql.tree.NullLiteral;
import cn.edu.ruc.iir.pard.sql.tree.StringLiteral;

import java.io.Serializable;

public abstract class Item
        implements Serializable
{
    /**
     *
     */
    private static final long serialVersionUID = 1L;
    public static Item clone(Item item)
    {
        if (item instanceof ValueItem) {
            return new ValueItem((ValueItem) item);
        }
        else if (item instanceof ColumnItem) {
            return new ColumnItem((ColumnItem) item);
        }
        return null;
    }
    public static Item parse(Expression expr)
    {
        if (expr instanceof DereferenceExpression) {
            DereferenceExpression de = (DereferenceExpression) expr;
            //System.out.println("base :" + de.getBase().getClass().getName());
            //System.out.println("field:" + de.getField());
            ColumnItem ci = new ColumnItem(de.getBase().toString(), de.getField().toString(), 0);
            return ci;
        }
        else if (expr instanceof Literal) {
            ValueItem value = new ValueItem(parseFromLiteral((Literal) expr));
            return value;
        }
        else {
            throw new NullPointerException("cannot parse the class " + expr.getClass().getName());
        }
        //return null;
    }
    public static Comparable parseFromLiteral(Literal literal)
    {
        if (literal instanceof LongLiteral) {
            return Long.parseLong(literal.toString());
        }
        else
            if (literal instanceof DoubleLiteral) {
                return Double.parseDouble(literal.toString());
            }
            else
                if (literal instanceof BooleanLiteral) {
                    return Boolean.parseBoolean(literal.toString());
                }
                else
                    if (literal instanceof CharLiteral) {
                        return literal.toString();
                    }
                    else
                        if (literal instanceof NullLiteral) {
                            return null;
                        }
                        else
                            if (literal instanceof StringLiteral) {
                                return literal.toString();
                            }
        return null;
    }
    public abstract String toString();

    public abstract int hashCode();
    public abstract boolean equals(Object o);
}
