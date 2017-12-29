package cn.edu.ruc.iir.pard.sql.expr;

import java.util.ArrayList;
import java.util.List;

public class CompositionExpr
        extends Expr
{
    private final List<Expr> conditions;
    private final LogicOperator logicOperator;
    public CompositionExpr(LogicOperator logicOperator)
    {
        super();
        conditions = new ArrayList<Expr>();
        this.logicOperator = logicOperator;
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
}
