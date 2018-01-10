package cn.edu.ruc.iir.pard.sql.expr;

import cn.edu.ruc.iir.pard.catalog.Condition;
import cn.edu.ruc.iir.pard.planner.ConditionComparator;
import cn.edu.ruc.iir.pard.sql.expr.Expr.LogicOperator;
import cn.edu.ruc.iir.pard.sql.expr.rules.MinimalItemLaw;
import cn.edu.ruc.iir.pard.sql.expr.rules.PushDownLaw;
import cn.edu.ruc.iir.pard.sql.expr.rules.TrueFalseLaw;
import cn.edu.ruc.iir.pard.sql.tree.BooleanLiteral;
import cn.edu.ruc.iir.pard.sql.tree.ComparisonExpression;
import cn.edu.ruc.iir.pard.sql.tree.Expression;
import cn.edu.ruc.iir.pard.sql.tree.LogicalBinaryExpression;
import cn.edu.ruc.iir.pard.sql.tree.NotExpression;

import java.io.Serializable;
import java.util.List;

public abstract class Expr
        implements Serializable
{
    /**
     *
     */
    private static final long serialVersionUID = 7848186477362630024L;
    public static PushDownLaw pdAnd = new PushDownLaw(LogicOperator.AND);
    public static PushDownLaw pdOr = new PushDownLaw(LogicOperator.OR);
    public static TrueFalseLaw tfLaw = new TrueFalseLaw();
    public static MinimalItemLaw milaw = new MinimalItemLaw();
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
    public static Expr parse(Condition cond, String tableName)
    {
        ColumnItem ci = new ColumnItem(tableName, cond.getColumnName(), cond.getDataType());
        ValueItem vi = new ValueItem(ConditionComparator.parseFromString(cond.getDataType(), cond.getValue()));
        return new SingleExpr(ci, vi, cond.getCompareType());
    }
    public static Expr and(Expr e1, Expr e2, LogicOperator opt)
    {
        CompositionExpr comp = new CompositionExpr(LogicOperator.AND);
        comp.getConditions().add(e1);
        comp.getConditions().add(e2);
        Expr and = null;
        if (opt == LogicOperator.AND) {
            and = pdAnd.apply(comp);
        }
        else {
            and = pdOr.apply(comp);
        }
        and = milaw.apply(and);
        and = tfLaw.apply(and);
        return and;
    }
    public static Expr parse(List<Condition> conditions, String tableName)
    {
        if (conditions.isEmpty()) {
            return new TrueExpr();
        }
        else if (conditions.size() == 1) {
            return parse(conditions.get(0), tableName);
        }
        else {
            CompositionExpr ce = new CompositionExpr(LogicOperator.AND);
            for (Condition c : conditions) {
                ce.getConditions().add(parse(c, tableName));
            }
            return ce;
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
    public abstract Expression toExpression();
}
