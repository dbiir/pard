package cn.edu.ruc.iir.pard.sql.expr.rules;

import cn.edu.ruc.iir.pard.catalog.GddUtil;
import cn.edu.ruc.iir.pard.sql.expr.CompositionExpr;
import cn.edu.ruc.iir.pard.sql.expr.Expr;
import cn.edu.ruc.iir.pard.sql.expr.Expr.LogicOperator;
import cn.edu.ruc.iir.pard.sql.expr.FalseExpr;
import cn.edu.ruc.iir.pard.sql.expr.SingleExpr;
import cn.edu.ruc.iir.pard.sql.expr.TrueExpr;
import cn.edu.ruc.iir.pard.sql.expr.UnaryExpr;
/**
 *de Morgan’s laws
 *
 *@author hagen
 * */
class PushDownNotLaw
        extends ExprLaw
{
    private static int parseComparedType(int type)
    {
        switch(type) {
            case GddUtil.compareEQUAL: return GddUtil.compareNOTEQUAL;
            case GddUtil.compareGREAT: return GddUtil.compareLESSEQUAL;
            case GddUtil.compareGREATEQUAL: return GddUtil.compareLESS;
            case GddUtil.compareLESS: return GddUtil.compareGREATEQUAL;
            case GddUtil.compareLESSEQUAL: return GddUtil.compareGREAT;
            case GddUtil.compareNOTEQUAL: return GddUtil.compareEQUAL;
        }
        throw new NullPointerException("cannot handle  comparator!");
    }
    @Override
    public Expr apply(Expr expr)
    {
        if (expr instanceof UnaryExpr) {
            UnaryExpr one = (UnaryExpr) expr;
            if (one.getCompareType() == LogicOperator.NOTHING) {
                return Expr.clone(one.getExpression());
            }
            else {
                Expr e = one.getExpression();
                if (e instanceof UnaryExpr) {
                    UnaryExpr ee = (UnaryExpr) e;
                    if (ee.getCompareType() == LogicOperator.NOT) {
                        return e;
                    }
                    else {
                        return this.apply(new UnaryExpr(LogicOperator.NOT, ee));
                    }
                }
                else if (e instanceof TrueExpr) {
                    return new FalseExpr();
                }
                else if (e instanceof FalseExpr) {
                    return new TrueExpr();
                }
                else if (e instanceof SingleExpr) {
                    //TODO:
                    SingleExpr se = new SingleExpr((SingleExpr) e);
                    SingleExpr single = new SingleExpr(se.getLvalue(), se.getRvalue(), parseComparedType(se.getCompareType()));
                    return single;
                }
                else if (e instanceof CompositionExpr) {
                    CompositionExpr ee = (CompositionExpr) e;
                    CompositionExpr comp = new CompositionExpr(LogicOperator.getReverse(ee.getLogicOperator()));
                    for (Expr chd : ee.getConditions()) {
                        comp.getConditions().add(this.apply(new UnaryExpr(LogicOperator.NOT, chd)));
                    }
                    return comp;
                }
                else {
                    throw new NullPointerException("cannot handled sub expr:" + e.getClass().getName());
                }
            }
        }
        else {
            return expr;
        }
    }
}
