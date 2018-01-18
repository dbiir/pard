package cn.edu.ruc.iir.pard.sql.expr;

import cn.edu.ruc.iir.pard.sql.tree.BooleanLiteral;
import cn.edu.ruc.iir.pard.sql.tree.Expression;
/**
 * TrueExpr
 *
 * It denotes an expression with value true..
 *
 * @author hagen
 * */
public class TrueExpr
        extends Expr
{
    /**
     *
     */
    private static final long serialVersionUID = -3006782887478595711L;
    private final String content = "True";
    public TrueExpr()
    {
    }
    public TrueExpr(TrueExpr expr)
    {
    }
    @Override
    public String toString()
    {
        return content;
    }
    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((content == null) ? 0 : content.hashCode());
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
        TrueExpr other = (TrueExpr) obj;
        if (content == null) {
            if (other.content != null) {
                return false;
            }
        }
        else if (!content.equals(other.content)) {
            return false;
        }
        return true;
    }
    @Override
    public Expression toExpression()
    {
        return new BooleanLiteral("True");
    }
}
