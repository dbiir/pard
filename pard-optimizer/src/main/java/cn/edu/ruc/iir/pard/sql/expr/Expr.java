package cn.edu.ruc.iir.pard.sql.expr;

import cn.edu.ruc.iir.pard.sql.tree.BooleanLiteral;
import cn.edu.ruc.iir.pard.sql.tree.ComparisonExpression;
import cn.edu.ruc.iir.pard.sql.tree.Expression;
import cn.edu.ruc.iir.pard.sql.tree.LogicalBinaryExpression;
import cn.edu.ruc.iir.pard.sql.tree.NotExpression;

import java.io.Serializable;

public abstract class Expr
        implements Serializable
{
    /**
     *
     */
    private static final long serialVersionUID = 7848186477362630024L;
    public static enum LogicOperator
    {
        AND("and"), OR("or"), NOT("!"), NOTHING("nothing");
        private LogicOperator(String strs)
        {
            str = strs;
        }
        private String str = null;
        public String toString()
        {
            return str;
        }
        public static LogicOperator getReverse(LogicOperator opt)
        {
            switch (opt) {
                case AND: return OR;
                case OR: return AND;
                case NOT: return LogicOperator.NOTHING;
                case NOTHING: return LogicOperator.NOT;
                default:
                    break;
            }
            throw new NullPointerException("cannot handle operator:" + opt);
        }
    }
    public Expr()
    {
    }
    public static Expr clone(Expr expr)
    {
        if (expr instanceof CompositionExpr) {
            return new CompositionExpr((CompositionExpr) expr);
        }
        else if (expr instanceof SingleExpr) {
            return new SingleExpr((SingleExpr) expr);
        }
        else if (expr instanceof UnaryExpr) {
            return new UnaryExpr((UnaryExpr) expr);
        }
        else if (expr instanceof TrueExpr) {
            return expr;
        }
        else if (expr instanceof FalseExpr) {
            return expr;
        }
        else {
            throw new NullPointerException("cannot clone the class " + expr.getClass().getName());
        }
    }
    public static Expr parse(Expression expr)
    {
        if (expr instanceof LogicalBinaryExpression) {
            return new CompositionExpr(expr);
        }
        else if (expr instanceof ComparisonExpression) {
            return new SingleExpr(expr);
        }
        else if (expr instanceof NotExpression) {
            return new UnaryExpr(expr);
        }
        else if (expr instanceof BooleanLiteral) {
            BooleanLiteral liter = (BooleanLiteral) expr;
            if (liter.getValue()) {
                return new TrueExpr();
            }
            else {
                return new FalseExpr();
            }
        }
        else {
            throw new NullPointerException("cannot parse the class " + expr.getClass().getName());
        }
        //return null;
    }
    public abstract String toString();
    public abstract int hashCode();
    public abstract boolean equals(Object o);
}
