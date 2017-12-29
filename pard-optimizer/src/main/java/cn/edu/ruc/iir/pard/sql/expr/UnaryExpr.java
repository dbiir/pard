package cn.edu.ruc.iir.pard.sql.expr;

public class UnaryExpr
        extends Expr
{
    private final LogicOperator compareType;
    private final Expr expression;
    public UnaryExpr(LogicOperator compareType, Expr expression)
    {
        super();
        this.compareType = compareType;
        this.expression = expression;
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
}
