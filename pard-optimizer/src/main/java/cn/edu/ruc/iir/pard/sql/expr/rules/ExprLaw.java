package cn.edu.ruc.iir.pard.sql.expr.rules;

import cn.edu.ruc.iir.pard.sql.expr.Expr;

public abstract class ExprLaw
{
    public ExprLaw()
    {
    }

    public abstract Expr apply(Expr expr);
}
