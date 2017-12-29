package cn.edu.ruc.iir.pard.sql.expr;

public abstract class Expr
{
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
    }
    public abstract String toString();
}
