package cn.edu.ruc.iir.pard.sql.expr;

import cn.edu.ruc.iir.pard.catalog.Condition;
import cn.edu.ruc.iir.pard.sql.expr.rules.ContainEliminateLaw;
import cn.edu.ruc.iir.pard.sql.expr.rules.MinimalItemLaw;
import cn.edu.ruc.iir.pard.sql.expr.rules.PushDownLaw;
import cn.edu.ruc.iir.pard.sql.expr.rules.TrueFalseLaw;
import cn.edu.ruc.iir.pard.sql.tree.BooleanLiteral;
import cn.edu.ruc.iir.pard.sql.tree.ComparisonExpression;
import cn.edu.ruc.iir.pard.sql.tree.Expression;
import cn.edu.ruc.iir.pard.sql.tree.LogicalBinaryExpression;
import cn.edu.ruc.iir.pard.sql.tree.NotExpression;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

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
    public static ContainEliminateLaw claw = new ContainEliminateLaw();
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
    public static Expr replace(Expr e1, ColumnItem from, ColumnItem to)
    {
        Expr e = Expr.clone(e1);
        if (e instanceof SingleExpr) {
            SingleExpr se = (SingleExpr) e;
            Item lv = se.getLvalue();
            Item rv = se.getRvalue();
            if (lv.equals(from)) {
                lv = Item.clone(to);
            }
            if (rv.equals(from)) {
                rv = Item.clone(to);
            }
            return new SingleExpr(lv, rv, se.getCompareType());
        }
        else if (e instanceof CompositionExpr) {
            CompositionExpr ce = (CompositionExpr) e;
            for (int i = 0; i < ce.getConditions().size(); i++) {
                Expr ex = ce.getConditions().get(i);
                ce.getConditions().set(i, replace(ex, from, to));
            }
            return ce;
        }
        else if (e instanceof UnaryExpr) {
            UnaryExpr ue = (UnaryExpr) e;
            return new UnaryExpr(ue.getCompareType(), replace(ue.getExpression(), from, to));
        }
        else if (e instanceof TrueExpr || e instanceof FalseExpr) {
            return e;
        }
        return e;
    }
    public static Expr replaceTableName(Expr e1, String from, String to)
    {
        Expr e = Expr.clone(e1);
        if (e instanceof SingleExpr) {
            System.out.println("from " + e.toString());
            SingleExpr se = (SingleExpr) e;
            Item lv = se.getLvalue();
            Item rv = se.getRvalue();
            if (lv instanceof ColumnItem && ((ColumnItem) lv).getTableName().equalsIgnoreCase(from)) {
                ColumnItem ci = (ColumnItem) lv;
                lv = new ColumnItem(to, ci.getColumnName(), ci.getDataType());
            }
            if (rv instanceof ColumnItem && ((ColumnItem) rv).getTableName().equalsIgnoreCase(from)) {
                ColumnItem ci = (ColumnItem) rv;
                rv = new ColumnItem(to, ci.getColumnName(), ci.getDataType());
            }
            se = new SingleExpr(lv, rv, se.getCompareType());
            //System.out.println("to " + se.toString());
            return se;
        }
        else if (e instanceof CompositionExpr) {
            CompositionExpr ce = (CompositionExpr) e;
            for (int i = 0; i < ce.getConditions().size(); i++) {
                Expr ex = ce.getConditions().get(i);
                ce.getConditions().set(i, replaceTableName(ex, from, to));
            }
            return ce;
        }
        else if (e instanceof UnaryExpr) {
            UnaryExpr ue = (UnaryExpr) e;
            return new UnaryExpr(ue.getCompareType(), replaceTableName(ue.getExpression(), from, to));
        }
        else if (e instanceof TrueExpr || e instanceof FalseExpr) {
            return e;
        }
        return e;
    }
    public static Expr generalReplace(Expr e1, Item from, Item to)
    {
        Expr e = Expr.clone(e1);
        if (e instanceof SingleExpr) {
            SingleExpr se = (SingleExpr) e;
            Item lv = se.getLvalue();
            Item rv = se.getRvalue();
            if (lv.equals(from)) {
                lv = Item.clone(to);
            }
            if (rv.equals(from)) {
                rv = Item.clone(to);
            }
            return new SingleExpr(lv, rv, se.getCompareType());
        }
        else if (e instanceof CompositionExpr) {
            CompositionExpr ce = (CompositionExpr) e;
            for (int i = 0; i < ce.getConditions().size(); i++) {
                Expr ex = ce.getConditions().get(i);
                ce.getConditions().set(i, generalReplace(ex, from, to));
            }
            return ce;
        }
        else if (e instanceof UnaryExpr) {
            UnaryExpr ue = (UnaryExpr) e;
            return new UnaryExpr(ue.getCompareType(), generalReplace(ue.getExpression(), from, to));
        }
        else if (e instanceof TrueExpr || e instanceof FalseExpr) {
            return e;
        }
        return e;
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
    private static List<Expr> extractList(Expr expr, String tableName, boolean rec)
    {
        List<Expr> list = new ArrayList<>();
        if (expr instanceof CompositionExpr) {
            CompositionExpr comp = (CompositionExpr) expr;
            for (Expr e : comp.getConditions()) {
                if (e instanceof SingleExpr) {
                    SingleExpr se = (SingleExpr) e;
                    if (se.getLvalue() instanceof ColumnItem && se.getRvalue() instanceof ValueItem) {
                        ColumnItem ci = (ColumnItem) se.getLvalue();
                        if (tableName != null && tableName.equalsIgnoreCase(ci.getTableName())) {
                            list.add(se);
                        }
                        else {
                            //System.out.println("cmp " + tableName + " " + ci.toString() + " False");
                        }
                    }
                }
                else if (rec) {
                    list.addAll(extractList(e, tableName, false));
                }
            }
        }
        else if (expr instanceof SingleExpr) {
            SingleExpr se = (SingleExpr) expr;
            if (se.getLvalue() instanceof ColumnItem && se.getRvalue() instanceof ValueItem) {
                ColumnItem ci = (ColumnItem) se.getLvalue();
                if (tableName != null && tableName.equals(ci.getTableName())) {
                    list.add(se);
                }
            }
        }
        return list;
    }
    // 可能有重复值
    public static List<String> extractTableColumn(Expr expr, String tableName)
    {
        Expr extractOr = pdAnd.apply(expr);
        Queue<Expr> traverse = new LinkedList<Expr>();
        Queue<SingleExpr> output = new LinkedList<SingleExpr>();
        List<String> out = new ArrayList<String>();
        traverse.add(extractOr);
        while (!traverse.isEmpty()) {
            Expr pop = traverse.poll();
            if (pop instanceof CompositionExpr) {
                traverse.addAll(((CompositionExpr) pop).getConditions());
            }
            else if (pop instanceof UnaryExpr) {
                traverse.add(((UnaryExpr) pop).getExpression());
            }
            else {
                output.add((SingleExpr) pop);
            }
        }
        while (!output.isEmpty()) {
            SingleExpr se = output.poll();
            Item lv = se.getLvalue();
            Item rv = se.getRvalue();
            if (lv instanceof ColumnItem && tableName.equals(((ColumnItem) lv).getTableName())) {
                out.add(((ColumnItem) lv).getColumnName());
            }
            if (rv instanceof ColumnItem && tableName.equals(((ColumnItem) rv).getTableName())) {
                out.add(((ColumnItem) rv).getColumnName());
            }
        }
        return out;
    }
    public static List<SingleExpr> extractTableJoinExpr(Expr expr)
    {
        Expr extractOr = pdOr.apply(expr);
        Queue<Expr> traverse = new LinkedList<Expr>();
        Queue<SingleExpr> output = new LinkedList<SingleExpr>();
        List<SingleExpr> out = new ArrayList<SingleExpr>();
        traverse.add(extractOr);
        while (!traverse.isEmpty()) {
            Expr pop = traverse.poll();
            if (pop instanceof CompositionExpr) {
                traverse.addAll(((CompositionExpr) pop).getConditions());
            }
            else if (pop instanceof UnaryExpr) {
                traverse.add(((UnaryExpr) pop).getExpression());
            }
            else {
                output.add((SingleExpr) pop);
            }
        }
        while (!output.isEmpty()) {
            SingleExpr se = output.poll();
            Item lv = se.getLvalue();
            Item rv = se.getRvalue();
            if (lv instanceof ColumnItem && rv instanceof ColumnItem) {
                out.add(se);
            }
        }
        return out;
    }
    //TODO: 从extractTableFilter的结果里提取垂直分片的信息。OK
    // 以及提取多表连接的条件
    // 以及 运用表连接条件和分片信息对表达式进一步化简
    public static Expr extractTableColumnFilter(Expr expr, List<String> projectList)
    {
        expr = pdOr.apply(expr);
        //只考虑两种情况，expr为SingleExpr和expr为 and的compositionExpr
        CompositionExpr and = new CompositionExpr(LogicOperator.AND);
        if (expr instanceof SingleExpr) {
            SingleExpr e = (SingleExpr) expr;
            ColumnItem ci = null;
            if (e.getLvalue() instanceof ColumnItem) {
                ci = (ColumnItem) e.getLvalue();
            }
            else {
                return new TrueExpr();
            }
            if (projectList.contains(ci.getColumnName())) {
                return expr;
            }
        }
        else {
            if (expr instanceof CompositionExpr && ((CompositionExpr) expr).getLogicOperator() == LogicOperator.AND) {
                CompositionExpr ce = (CompositionExpr) expr;
                for (Expr sub : ce.getConditions()) {
                    if (sub instanceof SingleExpr) {
                        SingleExpr e = (SingleExpr) sub;
                        ColumnItem ci = null;
                        if (e.getLvalue() instanceof ColumnItem) {
                            ci = (ColumnItem) e.getLvalue();
                        }
                        if (projectList.contains(ci.getColumnName())) {
                            and.getConditions().add(sub);
                        }
                    }
                }
                if (and.getConditions().size() == 1) {
                    return and.getConditions().get(0);
                }
                else if (and.getConditions().size() > 1) {
                    return and;
                }
            }
        }
        return new TrueExpr();
    }
    public static Expr extractTableFilter(Expr expr, String tableName)
    {
        Expr extractOr = pdAnd.apply(expr);
        //System.out.println("or:" + extractOr);
        Expr extractAnd = pdOr.apply(extractOr);
        //System.out.println("and" + extractAnd);
        CompositionExpr and = new CompositionExpr(LogicOperator.AND);
        CompositionExpr or = new CompositionExpr(LogicOperator.OR);
        //and.getConditions().addAll(extractList(extractAnd, tableName, false));
        //or.getConditions().addAll(extractList(extractOr, tableName, true));
        for (Expr sube : extractList(extractAnd, tableName, false)) {
            PushDownLaw.add(and.getConditions(), sube);
        }
        for (Expr sube : extractList(extractOr, tableName, true)) {
            PushDownLaw.add(or.getConditions(), sube);
        }
        if (and.getConditions().size() > 0) {
            if (or.getConditions().size() > 0) {
                and.getConditions().add(or);
            }
            return and; //optimize(and, LogicOperator.);
        }
        else if (or.getConditions().size() > 0) {
            return or;
        }
        return new TrueExpr();
    }
    public static Expr parse(Condition cond, String tableName)
    {
        ColumnItem ci = new ColumnItem(tableName, cond.getColumnName(), cond.getDataType());
        ValueItem vi = new ValueItem(ConditionComparator.parseFromString(cond.getDataType(), cond.getValue()));
        return new SingleExpr(ci, vi, cond.getCompareType());
    }
    public static Expr optimize(Expr e1, LogicOperator opt)
    {
        Expr and = null;
        if (opt == LogicOperator.AND) {
            and = pdAnd.apply(e1);
        }
        else {
            and = pdOr.apply(e1);
        }
        and = claw.apply(and);
        and = milaw.apply(and);
        and = tfLaw.apply(and);
        return and;
    }
    public static Expr and(Expr e1, Expr e2, LogicOperator opt)
    {
        CompositionExpr comp = new CompositionExpr(LogicOperator.AND);
        comp.getConditions().add(e1);
        comp.getConditions().add(e2);
        comp = PushDownLaw.formatExpr(comp);
        Expr and = optimize(comp, opt);
        return and;
    }
    public static Expr or(Expr e1, Expr e2, LogicOperator opt)
    {
        CompositionExpr comp = new CompositionExpr(LogicOperator.OR);
        comp.getConditions().add(e1);
        comp.getConditions().add(e2);
        comp = PushDownLaw.formatExpr(comp);
        Expr and = optimize(comp, opt);
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
