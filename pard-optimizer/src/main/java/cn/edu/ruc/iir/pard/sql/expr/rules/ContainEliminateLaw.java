package cn.edu.ruc.iir.pard.sql.expr.rules;

import cn.edu.ruc.iir.pard.sql.expr.CompositionExpr;
import cn.edu.ruc.iir.pard.sql.expr.Expr;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ContainEliminateLaw
        extends ExprLaw
{
    public boolean isParent(Expr parent, Expr children)
    {
        if (!(children instanceof CompositionExpr) && parent instanceof CompositionExpr) {
            CompositionExpr expr = (CompositionExpr) parent;
            for (Expr e : expr.getConditions()) {
                if (children.equals(e)) {
                    return true;
                }
            }
        }
        else if (children instanceof CompositionExpr && parent instanceof CompositionExpr) {
            CompositionExpr p = (CompositionExpr) parent;
            CompositionExpr c = (CompositionExpr) children;
            if (c.getConditions().size() > p.getConditions().size()) {
                System.out.println("warning: cmp un optimized!");
                return false;
            }
            for (Expr ce : c.getConditions()) {
                boolean isIncludedByParent = false;
                for (Expr pe : p.getConditions()) {
                    if (ce.equals(pe)) {
                        isIncludedByParent = true;
                    }
                }
                if (!isIncludedByParent) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }
    @Override
    public Expr apply(Expr expr)
    {
        if (expr instanceof CompositionExpr) {
            CompositionExpr ce = (CompositionExpr) expr;
            CompositionExpr ret = new CompositionExpr(ce.getLogicOperator());
            Map<Expr, Expr> parent = new HashMap<Expr, Expr>();
            List<Expr> singleExpr = new ArrayList<Expr>();
            List<CompositionExpr> compExpr = new ArrayList<CompositionExpr>();
            for (Expr tp : ce.getConditions()) {
                if (tp instanceof CompositionExpr) {
                    compExpr.add((CompositionExpr) tp);
                }
                else {
                    singleExpr.add(tp);
                }
            }
            compExpr.sort((x, y)->x.getConditions().size() - y.getConditions().size());
            for (Expr single : singleExpr) {
                for (CompositionExpr comp : compExpr) {
                    if (isParent(comp, single)) {
                        parent.put(single, comp);
                    }
                }
            }
            for (int i = 0; i < compExpr.size() - 1; i++) {
                CompositionExpr little = compExpr.get(i);
                for (int j = i + 1; j < compExpr.size(); j++) {
                    CompositionExpr big = compExpr.get(j);
                    if (isParent(big, little)) {
                        parent.put(little, big);
                    }
                }
            }
            for (Expr tp : ce.getConditions()) {
                if (parent.get(tp) == null) {
                    ret.getConditions().add(tp);
                }
            }
            return ret;
            /*
            for (Expr tp : ce.getConditions()) {
                for (Expr tc : ce.getConditions()) {
                    if(tp==tc) continue;
                    //检测tc的parent是不是tp
                    //如果tc已经在map中，则不检查该元素
                    if (parent.get(tc) != null) {
                        continue;
                    }
                    if (isParent(tp, tc)) {
                        parent.put(tc, tp);
                    }
                }
            }*/
        }
        else {
            return Expr.clone(expr);
        }
    }
}
