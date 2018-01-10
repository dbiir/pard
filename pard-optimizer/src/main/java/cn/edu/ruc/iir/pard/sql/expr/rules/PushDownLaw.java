package cn.edu.ruc.iir.pard.sql.expr.rules;

import cn.edu.ruc.iir.pard.sql.expr.CompositionExpr;
import cn.edu.ruc.iir.pard.sql.expr.Expr;
import cn.edu.ruc.iir.pard.sql.expr.Expr.LogicOperator;
import cn.edu.ruc.iir.pard.sql.expr.FalseExpr;
import cn.edu.ruc.iir.pard.sql.expr.SingleExpr;
import cn.edu.ruc.iir.pard.sql.expr.TrueExpr;
import cn.edu.ruc.iir.pard.sql.expr.UnaryExpr;

import java.util.ArrayList;
import java.util.List;

public class PushDownLaw
        extends ExprLaw
{
    private LogicOperator operator;
    private static int cnt = 0;
    private static PushDownNotLaw notLaw = new PushDownNotLaw();
    public PushDownLaw(LogicOperator operator)
    {
        super();
        this.operator = operator;
    }

    public LogicOperator getOperator()
    {
        return operator;
    }
    public static void add(List<Expr> list, Expr e)
    {
        //boolean eq = false;
        for (Expr it : list) {
            if (it.getClass() == e.getClass() && it.hashCode() == e.hashCode()) {
                if (it.equals(e)) {
                    return;
                }
            }
        }
        list.add(e);
    }
    public static CompositionExpr formatExpr(CompositionExpr expr)
    {
        CompositionExpr exprs = new CompositionExpr(expr);
        CompositionExpr ret = new CompositionExpr(exprs.getLogicOperator());
        for (Expr e : exprs.getConditions()) {
            if (e instanceof CompositionExpr && ((CompositionExpr) e).getLogicOperator() == expr.getLogicOperator()) {
                //ret.getConditions().addAll();
                for (Expr ex : ((CompositionExpr) e).getConditions()) {
                    add(ret.getConditions(), ex);
                }
            }
            else if (e instanceof CompositionExpr) {
                e = formatExpr((CompositionExpr) e);
                add(ret.getConditions(), e);
            }
            else {
                add(ret.getConditions(), e);
            }
        }
        return ret;
    }
    public void printLog(CompositionExpr e, LogicOperator op)
    {
        System.out.println(e.toString() + " op " + op.toString());
    }
    private Expr doBefore(Expr expr, int k)
    {
        //System.out.println(k + " before " + expr.toString());
        return expr;
    }
    private Expr doAfter(Expr expr, int k, int linenum)
    {
        //System.out.println(k + " " + linenum + " after " + expr.toString());
        return expr;
    }
    @Override
    public Expr apply(Expr expr)
    {
        int k = cnt++;
        doBefore(expr, k);
        if (expr instanceof SingleExpr || expr instanceof TrueExpr || expr instanceof FalseExpr) {
            return doAfter(Expr.clone(expr), k, 83);
        }
        else if (expr instanceof CompositionExpr) {
            CompositionExpr ce = (CompositionExpr) expr;
            //printLog(ce, this.operator);
            if (ce.getLogicOperator() == this.operator) {
                //当前符号opt是要下推的符号
                //选取一个子集，子集为CompositionExpr，且子集的符号不是要下推的符号，以此应用分配律
                CompositionExpr subExpr = null;
                List<Expr> others = new ArrayList<Expr>();
                for (Expr e : ce.getConditions()) {
                    if (e instanceof CompositionExpr && ((CompositionExpr) e).getLogicOperator() != this.operator) {
                        if (subExpr == null) {
                            subExpr = (CompositionExpr) e;
                            continue;
                        }
                    }
                    Expr ea = this.apply(Expr.clone(e));
                    others.add(ea);
                }
                //System.out.println("subExpr" + subExpr);
                if (subExpr != null) {
                    CompositionExpr ret = new CompositionExpr(subExpr.getLogicOperator());
                    for (Expr e : subExpr.getConditions()) {
                        ArrayList<Expr> list = new ArrayList<Expr>();
                        list.addAll(others);
                        list.add(this.apply(e));
                        CompositionExpr child = new CompositionExpr(this.operator);
                        child.getConditions().addAll(list);
                        ret.getConditions().add(this.apply(child));
                    }
                    return doAfter(formatExpr(ret), k, 114);
                }
                else {
                    CompositionExpr ret = new CompositionExpr(ce.getLogicOperator());
                    boolean needApply = false;
                    for (Expr e : ce.getConditions()) {
                        Expr ae = this.apply(e);
                        ret.getConditions().add(ae);
                        if (ae instanceof CompositionExpr && ((CompositionExpr) ae).getLogicOperator() != this.operator) {
                            needApply = true;
                        }
                    }
                    Expr e = null;
                    if (needApply) {
                        e = this.apply(ret);
                    }
                    else {
                        e = ret;
                    }
                    if (e instanceof CompositionExpr) {
                        ret = formatExpr((CompositionExpr) e);
                        return doAfter(ret, k, 124);
                    }
                    else {
                        return e;
                    }
                }
            }
            else {
                CompositionExpr ret = new CompositionExpr(ce.getLogicOperator());
                for (Expr e : ce.getConditions()) {
                    ret.getConditions().add(this.apply(e));
                }
                return doAfter(formatExpr(ret), k, 129);
            }
        }
        else if (expr instanceof UnaryExpr) {
            UnaryExpr e = (UnaryExpr) expr;
            Expr inner = this.apply(notLaw.apply(e));
            return doAfter(inner, k, 135);
           // Expr inner = this.apply(e.getExpression());
            //return doAfter(new UnaryExpr(e.getCompareType(), inner), k);
        }
        else {
            throw new NullPointerException("cannot parse class:" + expr.getClass().getName());
        }
    }
}
