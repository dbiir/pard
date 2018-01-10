package cn.edu.ruc.iir.pard.sql.expr;

import cn.edu.ruc.iir.pard.sql.tree.BooleanLiteral;
import cn.edu.ruc.iir.pard.sql.tree.CharLiteral;
import cn.edu.ruc.iir.pard.sql.tree.DoubleLiteral;
import cn.edu.ruc.iir.pard.sql.tree.Expression;
import cn.edu.ruc.iir.pard.sql.tree.Literal;
import cn.edu.ruc.iir.pard.sql.tree.LogicalBinaryExpression;
import cn.edu.ruc.iir.pard.sql.tree.LogicalBinaryExpression.Type;
import cn.edu.ruc.iir.pard.sql.tree.LongLiteral;
import cn.edu.ruc.iir.pard.sql.tree.NullLiteral;
import cn.edu.ruc.iir.pard.sql.tree.StringLiteral;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class CompositionExpr
        extends Expr
{
    private List<Expr> conditions;
    private LogicOperator logicOperator;
    public CompositionExpr(CompositionExpr expr)
    {
        this.logicOperator = expr.logicOperator;
        conditions = new ArrayList<Expr>();
        for (Expr e : expr.conditions) {
            conditions.add(Expr.clone(e));
        }
    }
    public CompositionExpr(LogicOperator logicOperator)
    {
        super();
        conditions = new ArrayList<Expr>();
        this.logicOperator = logicOperator;
    }
    public CompositionExpr(Expression expr)
    {
        //super(expr);
        conditions = new ArrayList<Expr>();
        LogicalBinaryExpression binary = (LogicalBinaryExpression) expr;
        if (binary.getType() == Type.AND) {
            this.logicOperator = Expr.LogicOperator.AND;
        }
        else if (binary.getType() == Type.OR) {
            this.logicOperator = Expr.LogicOperator.OR;
        }
        //pase left parse right.
        Expr left = Expr.parse(binary.getLeft());
        Expr right = Expr.parse(binary.getRight());
        conditions.add(left);
        conditions.add(right);
    }
    public LogicOperator getLogicOperator()
    {
        return logicOperator;
    }

    public List<Expr> getConditions()
    {
        return conditions;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("( ");
        for (int i = 0; i < conditions.size(); i++) {
            Expr expr = conditions.get(i);
            sb.append(expr);
            if (i != conditions.size() - 1) {
                sb.append(" ").append(logicOperator).append(" ");
            }
        }
        sb.append(" )");
        return sb.toString();
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
    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((conditions == null) ? 0 : conditions.hashCode());
        result = prime * result + ((logicOperator == null) ? 0 : logicOperator.hashCode());
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
        CompositionExpr other = (CompositionExpr) obj;
        if (conditions == null) {
            if (other.conditions != null) {
                return false;
            }
        }
        else {
            if (!conditions.equals(other.conditions)) {
                return false;
            }
        }
        if (logicOperator != other.logicOperator) {
            return false;
        }
        return true;
    }
    @Override
    public Expression toExpression()
    {
        Queue<Expression> que = new LinkedList<Expression>();
        Type t = null;
        if (this.logicOperator == LogicOperator.AND) {
            t = Type.AND;
        }
        else {
            t = Type.OR;
        }
        for (Expr e : this.getConditions()) {
            que.add(e.toExpression());
        }
        while (que.size() > 1) {
            Expression lv = que.poll();
            Expression rv = que.poll();
            que.add(new LogicalBinaryExpression(t, lv, rv));
        }
        return que.poll();
    }
}
