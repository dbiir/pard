package cn.edu.ruc.iir.pard.sql.expr;

import cn.edu.ruc.iir.pard.sql.tree.Expression;
import cn.edu.ruc.iir.pard.sql.tree.NotExpression;

public class UnaryExpr
        extends Expr
{
    private final LogicOperator compareType;
    private final Expr expression;
    public UnaryExpr(UnaryExpr expr)
    {
        this.compareType = expr.compareType;
        this.expression = Expr.clone(expr.expression);
    }
    public UnaryExpr(LogicOperator compareType, Expr expression)
    {
        super();
        this.compareType = compareType;
        this.expression = expression;
    }
    public UnaryExpr(Expression expr)
    {
        if (expr instanceof NotExpression) {
            NotExpression not = (NotExpression) expr;
            compareType = LogicOperator.NOT;
            expression = Expr.parse(not.getValue());
        }
        else {
            compareType = LogicOperator.NOTHING;
            expression = Expr.parse(expr);
        }
    }
    public LogicOperator getCompareType()
    {
        return compareType;
    }
    public Expr getExpression()
    {
        return expression;
    }
    @Override
    public String toString()
    {
        return "( " + compareType.toString() + expression.toString() + " )";
    }
    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((compareType == null) ? 0 : compareType.hashCode());
        result = prime * result + ((expression == null) ? 0 : expression.hashCode());
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
        UnaryExpr other = (UnaryExpr) obj;
        if (compareType != other.compareType) {
            return false;
        }
        if (expression == null) {
            if (other.expression != null) {
                return false;
            }
        }
        else if (!expression.equals(other.expression)) {
            return false;
        }
        return true;
    }
    @Override
    public Expression toExpression()
    {
        if (compareType == LogicOperator.NOTHING) {
            return expression.toExpression();
        }
        else {
            return new NotExpression(expression.toExpression());
        }
    }
}
