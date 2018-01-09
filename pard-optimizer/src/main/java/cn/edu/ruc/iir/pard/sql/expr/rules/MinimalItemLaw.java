package cn.edu.ruc.iir.pard.sql.expr.rules;

import cn.edu.ruc.iir.pard.catalog.GddUtil;
import cn.edu.ruc.iir.pard.sql.expr.ColumnItem;
import cn.edu.ruc.iir.pard.sql.expr.CompositionExpr;
import cn.edu.ruc.iir.pard.sql.expr.Expr;
import cn.edu.ruc.iir.pard.sql.expr.Expr.LogicOperator;
import cn.edu.ruc.iir.pard.sql.expr.FalseExpr;
import cn.edu.ruc.iir.pard.sql.expr.SingleExpr;
import cn.edu.ruc.iir.pard.sql.expr.TrueExpr;
import cn.edu.ruc.iir.pard.sql.expr.ValueItem;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class MinimalItemLaw
        extends ExprLaw
{
    public static final int EQ = 4;
    public static final int GT = 1;
    public static final int GE = 5;
    public static final int LT = 2;
    public static final int LE = 6;
    public static final int NE = 3;
    public static final int FULL = 7;
    public static final int VACANT = 0;
    public static int toGDDType(int cmpType)
    {
        switch(cmpType) {
            case EQ: return GddUtil.compareEQUAL;
            case GT: return GddUtil.compareGREAT;
            case GE: return GddUtil.compareGREATEQUAL;
            case LT: return GddUtil.compareLESS;
            case LE: return GddUtil.compareLESSEQUAL;
            case NE: return GddUtil.compareNOTEQUAL;
        }
        return 0;
    }
    public static int cmpMask(int cmpType)
    {
        // gt : 0b001; 1
        // ge: 0b101; 5
        // lt: 0b010;   2
        // le: 0b110;  6
        // eq: 0b100;4
        // ne: 0b011; 3
        // gt|le == ge|lt ==ne|eq ==0b111;
        //gt xor ge == lt xor le ==  0b100
        // ge = gt|eq,le = lt|eq
        switch(cmpType) {
            case GddUtil.compareEQUAL: return EQ;
            case GddUtil.compareGREAT: return GT;
            case GddUtil.compareGREATEQUAL: return GE;
            case GddUtil.compareLESS: return LT;
            case GddUtil.compareLESSEQUAL: return LE;
            case GddUtil.compareNOTEQUAL: return NE;
        }
        return VACANT;
    }
    public List<Expr> parseSameOptOrMerge(SingleExpr expr1, SingleExpr expr2, List<Expr> expr)
    {
        ValueItem rv1 = (ValueItem) expr1.getRvalue();
        ValueItem rv2 = (ValueItem) expr2.getRvalue();
        int cmpMask1 = cmpMask(expr1.getCompareType());
        int cmpMask2 = cmpMask(expr2.getCompareType());
        switch(cmpMask1 | EQ) {
            case GE:
                if (rv1.biggerThan(rv2)) {
                    expr.add(expr2);
                }
                else {
                    expr.add(expr1);
                }
                return expr;
            case LE:
                if (rv1.smallerThan(rv2)) {
                    expr.add(expr2);
                }
                else {
                    expr.add(expr1);
                }
                return expr;
            case EQ:
                if (rv1.equals(rv2)) {
                    expr.add(expr1);
                }
                else {
                    expr.add(expr1);
                    expr.add(expr2);
                }
                return expr;
            case FULL:
                if (rv1.equals(rv2)) {
                    expr.add(expr1);
                }
                else {
                    expr.add(new TrueExpr());
                }
                return expr;
        }
        //TODO : not reached!
        throw new NullPointerException("not reached!");
       // return expr;
    }
    public List<Expr> parseSameOptAndMerge(SingleExpr expr1, SingleExpr expr2, List<Expr> expr)
    {
        ValueItem rv1 = (ValueItem) expr1.getRvalue();
        ValueItem rv2 = (ValueItem) expr2.getRvalue();
        int cmpMask1 = cmpMask(expr1.getCompareType());
        int cmpMask2 = cmpMask(expr2.getCompareType());
        switch(cmpMask1 | EQ) {
            case GE:
                if (rv1.biggerThan(rv2)) {
                    expr.add(expr1);
                }
                else {
                    expr.add(expr2);
                }
                return expr;
            case LE:
                if (rv1.smallerThan(rv2)) {
                    expr.add(expr1);
                }
                else {
                    expr.add(expr2);
                }
                return expr;
            case EQ:
                if (rv1.equals(rv2)) {
                    expr.add(expr1);
                }
                else {
                    expr.add(new FalseExpr());
                }
                return expr;
            case FULL:
                if (rv1.equals(rv2)) {
                    expr.add(expr1);
                }
                else {
                    expr.add(expr1);
                    expr.add(expr2);
                }
                return expr;
        }
        throw new NullPointerException("not reached!");
        //return expr;
    }
    public List<Expr> mergeTwo(SingleExpr expr1, SingleExpr expr2, LogicOperator opt)
    {
        List<Expr> expr = new ArrayList<Expr>();
        ValueItem rv1 = (ValueItem) expr1.getRvalue();
        ValueItem rv2 = (ValueItem) expr2.getRvalue();
        int cmpMask1 = cmpMask(expr1.getCompareType());
        int cmpMask2 = cmpMask(expr2.getCompareType());
        if ((cmpMask1 | EQ) == (cmpMask2 | EQ)) {
            if (opt == LogicOperator.AND) {
                return parseSameOptAndMerge(expr1, expr2, expr);
            }
            else {
                return parseSameOptOrMerge(expr1, expr2, expr);
            }
        }
        else if (rv1.equals(rv2) && ((cmpMask1 & NE) | (cmpMask2 & NE)) == NE) {
            if (opt == LogicOperator.AND) {
                if (cmpMask1 == NE || cmpMask2 == NE) {
                    SingleExpr notNE = null;
                    int cmpMask = 0;
                    if (cmpMask1 == NE && cmpMask2 == NE) {
                        expr.add(expr1);
                        return expr;
                    }
                    if (cmpMask1 != NE) {
                        notNE = expr1;
                        cmpMask = cmpMask1;
                    }
                    else if (cmpMask2 != NE) {
                        notNE = expr2;
                        cmpMask = cmpMask2;
                    }
                    if ((cmpMask & EQ) == EQ) {
                        if (cmpMask == EQ) {
                            expr.add(new FalseExpr());
                            return expr;
                        }
                        else {
                            SingleExpr tmp = new SingleExpr(notNE.getLvalue(), notNE.getRvalue(), toGDDType(cmpMask & NE));
                            expr.add(tmp);
                            return expr;
                        }
                    }
                    else {
                        expr.add(notNE);
                        return expr;
                    }
                }
                else if ((cmpMask1 & cmpMask2) != EQ) {
                    expr.add(new FalseExpr());
                }
                else {
                    SingleExpr se = new SingleExpr(expr1.getLvalue(), rv1, GddUtil.compareEQUAL);
                    expr.add(se);
                }
                return expr;
            }
            else {
                //opt == or
                if (cmpMask1 == NE || cmpMask2 == NE) {
                    SingleExpr notNE = null;
                    int cmpMask = 0;
                    if (cmpMask1 == NE && cmpMask2 == NE) {
                        expr.add(expr1);
                        return expr;
                    }
                    if (cmpMask1 != NE) {
                        notNE = expr2;
                        cmpMask = cmpMask1;
                    }
                    else if (cmpMask2 != NE) {
                        notNE = expr1;
                        cmpMask = cmpMask2;
                    }
                    if ((cmpMask & EQ) == EQ) {
                        expr.add(new TrueExpr());
                        return expr;
                    }
                    else {
                        expr.add(notNE);
                        return expr;
                    }
                }
                else if (((cmpMask1 | cmpMask2) & EQ) != EQ) {
                    expr.add(new TrueExpr());
                }
                else {
                    SingleExpr se = new SingleExpr(expr1.getLvalue(), rv1, GddUtil.compareNOTEQUAL);
                    expr.add(se);
                }
                return expr;
            }
        }
        else if (((cmpMask1 & NE) | (cmpMask2 & NE)) == NE) {
            if (cmpMask1 == NE || cmpMask2 == NE) {
                if (cmpMask1 == cmpMask2) {
                    if (opt == LogicOperator.OR) {
                        expr.add(new TrueExpr());
                        return expr;
                    }
                }
                else {
                    SingleExpr notNE = null;
                    SingleExpr ne = null;
                    int cmpMask = 0;
                    if (cmpMask1 == NE && cmpMask2 == NE) {
                        expr.add(expr1);
                        return expr;
                    }
                    if (cmpMask1 != NE) {
                        notNE = expr1;
                        ne = expr2;
                        cmpMask = cmpMask1;
                    }
                    else if (cmpMask2 != NE) {
                        notNE = expr2;
                        ne = expr2;
                        cmpMask = cmpMask2;
                    }
                    ValueItem vi = (ValueItem) notNE.getRvalue();
                    ValueItem test = (ValueItem) ne.getRvalue();
                    boolean flag = TrueFalseLaw.cmp(notNE.getCompareType(), vi, test);
                    if (flag) {
                        if (opt == LogicOperator.OR) {
                            expr.add(new TrueExpr());
                            return expr;
                        }
                    }
                    else {
                        if (opt == LogicOperator.OR) {
                            expr.add(ne);
                            return expr;
                        }
                        else {
                            expr.add(notNE);
                            return expr;
                        }
                    }
                }
            }
            else {
                SingleExpr bt = null;
                SingleExpr lt = null;
                if ((cmpMask1 | EQ) == GE) {
                    bt = expr1;
                    lt = expr2;
                }
                else {
                    bt = expr2;
                    lt = expr1;
                }
                ValueItem btr = (ValueItem) bt.getRvalue();
                ValueItem ltr = (ValueItem) lt.getRvalue();
                if (TrueFalseLaw.cmp(GddUtil.compareGREAT, btr, ltr)) {
                    if (opt == LogicOperator.AND) {
                        expr.add(new FalseExpr());
                        return expr;
                    }
                }
                else {
                    if (opt == LogicOperator.OR) {
                        expr.add(new TrueExpr());
                        return expr;
                    }
                }
            }
        }
        expr.add(expr1);
        expr.add(expr2);
        return expr;
    }
    public List<Expr> merge(List<SingleExpr> singleExpr, LogicOperator operator)
    {
        LinkedList<SingleExpr> que = new LinkedList<SingleExpr>();
        for (SingleExpr se : singleExpr) {
            que.add(se);
        }
        int num = que.size() - 1;
        for (int i = 0; i < que.size() - 1; i++) {
            SingleExpr expr = que.poll();
            for (int j = 0; j < num; j++) {
                SingleExpr e = que.poll();
                List<Expr> m = mergeTwo(expr, e, operator);
                if (m.size() >= 2) {
                    que.add(e);
                }
                else {
                    Expr cob = m.get(0);
                    if (cob instanceof SingleExpr) {
                        num--;
                        que.add((SingleExpr) cob);
                        break;
                    }
                    else if (cob instanceof TrueExpr && operator == LogicOperator.OR) {
                        return m;
                    }
                    else if (cob instanceof FalseExpr && operator == LogicOperator.AND) {
                        return m;
                    }
                }
            }
            num--;
            que.add(expr);
        }
        List<Expr> list = new ArrayList<Expr>();
        que.forEach(list::add);
        return list;
    }
    @Override
    public Expr apply(Expr expr)
    {
        if (expr instanceof CompositionExpr) {
            CompositionExpr ce = (CompositionExpr) expr;
            CompositionExpr ret = new CompositionExpr(ce.getLogicOperator());
            Map<Expr, Boolean> merged = new HashMap<Expr, Boolean>();
            List<SingleExpr> singleExpr = new ArrayList<SingleExpr>();
            List<CompositionExpr> compExpr = new ArrayList<CompositionExpr>();
            List<Expr> others = new ArrayList<Expr>();
            for (Expr tp : ce.getConditions()) {
                if (tp instanceof CompositionExpr) {
                    compExpr.add((CompositionExpr) tp);
                }
                else if (tp instanceof SingleExpr) {
                    singleExpr.add((SingleExpr) tp);
                }
                else {
                    others.add(tp);
                }
            }
            ret.getConditions().addAll(others);
            for (CompositionExpr cep : compExpr) {
                ret.getConditions().add(this.apply(cep));
            }
            Map<ColumnItem, List<SingleExpr>> colmap = new HashMap<ColumnItem, List<SingleExpr>>();
            for (SingleExpr s : singleExpr) {
                if (s.getLvalue() instanceof ColumnItem && s.getRvalue() instanceof ValueItem) {
                    ColumnItem ci = (ColumnItem) s.getLvalue();
                    ValueItem vi = (ValueItem) s.getRvalue();
                    List<SingleExpr> group = colmap.get(ci);
                    if (group == null) {
                        group = new ArrayList<SingleExpr>();
                        colmap.put(ci, group);
                    }
                    group.add(s);
                }
                else {
                    ret.getConditions().add(s);
                }
            }
            for (List<SingleExpr> group : colmap.values()) {
                if (group.size() <= 1) {
                    ret.getConditions().addAll(group);
                }
                else {
                    ret.getConditions().addAll(merge(group, ce.getLogicOperator()));
                }
            }
            return ret;
        }
        else {
            return Expr.clone(expr);
        }
    }
}
