package cn.edu.ruc.iir.pard.sql.expr.rules;

import cn.edu.ruc.iir.pard.catalog.GddUtil;
import cn.edu.ruc.iir.pard.sql.expr.CompositionExpr;
import cn.edu.ruc.iir.pard.sql.expr.Expr;
import cn.edu.ruc.iir.pard.sql.expr.Expr.LogicOperator;
import cn.edu.ruc.iir.pard.sql.expr.FalseExpr;
import cn.edu.ruc.iir.pard.sql.expr.SingleExpr;
import cn.edu.ruc.iir.pard.sql.expr.TrueExpr;
import cn.edu.ruc.iir.pard.sql.expr.UnaryExpr;
import cn.edu.ruc.iir.pard.sql.expr.ValueItem;

public class TrueFalseLaw
        extends ExprLaw
{
    private static PushDownNotLaw notLaw = new PushDownNotLaw();
    @Override
    public Expr apply(Expr expr)
    {
        if (expr instanceof TrueExpr || expr instanceof FalseExpr) {
            return expr;
        }
        else if (expr instanceof UnaryExpr) {
            return this.apply(notLaw.apply(expr));
        }
        else if (expr instanceof SingleExpr) {
            return parseSingle((SingleExpr) expr);
        }
        else if (expr instanceof CompositionExpr) {
            CompositionExpr ce = (CompositionExpr) expr;
            CompositionExpr ret = new CompositionExpr(ce.getLogicOperator());
            if (ce.getLogicOperator() == LogicOperator.AND) {
                for (Expr sub : ce.getConditions()) {
                    sub = this.apply(sub);
                    //System.out.println("apply:" + sub);
                    if (sub instanceof FalseExpr) {
                        return sub;
                    }
                    else if (!(sub instanceof TrueExpr)) {
                        ret.getConditions().add(sub);
                    }
                }
                if (ret.getConditions().isEmpty()) {
                    return new TrueExpr();
                }
                return ret;
            }
            else if (ce.getLogicOperator() == LogicOperator.OR) {
                for (Expr sub : ce.getConditions()) {
                    sub = this.apply(sub);
                    if (sub instanceof TrueExpr) {
                        return sub;
                    }
                    else if (!(sub instanceof FalseExpr)) {
                        ret.getConditions().add(sub);
                    }
                }
                if (ret.getConditions().isEmpty()) {
                    return new FalseExpr();
                }
                return ret;
            }
            else {
                throw new NullPointerException("unkown operator!");
            }
        }
        else {
            throw new NullPointerException("class " + expr.getClass().getName() + " can not judge by true/false law");
        }
    }
    public static Boolean cmp(int cmpType, ValueItem lv, ValueItem rv)
    {
        switch (cmpType) {
            case GddUtil.compareEQUAL:
                return lv.equals(rv);
            case GddUtil.compareGREAT:
                return lv.biggerThan(rv);
            case GddUtil.compareGREATEQUAL:
                return lv.biggerThan(rv) || lv.equals(rv);
            case GddUtil.compareLESS:
                return lv.smallerThan(rv);
            case GddUtil.compareLESSEQUAL:
                return lv.smallerThan(rv) || lv.equals(rv);
            case GddUtil.compareNOTEQUAL:
                return !lv.equals(rv);
            default:
        }

        return null;
    }
    public Expr parseSingle(SingleExpr expr)
    {
        if (expr.getLvalue() instanceof ValueItem && expr.getRvalue() instanceof ValueItem) {
            ValueItem lv = (ValueItem) expr.getLvalue();
            ValueItem rv = (ValueItem) expr.getRvalue();
            try {
                if (cmp(expr.getCompareType(), lv, rv)) {
                    return new TrueExpr();
                }
                else {
                    return new FalseExpr();
                }
            }
            catch (Exception e) {
                e.printStackTrace();
                return expr;
            }
        }
        else {
            return expr;
        }
    }
}
