package cn.edu.ruc.iir.pard.sql.expr;

public class FalseExpr
        extends Expr
{
    private final String content = "False";
    public FalseExpr()
    {
    }

    public FalseExpr(FalseExpr expr)
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
        FalseExpr other = (FalseExpr) obj;
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
}
