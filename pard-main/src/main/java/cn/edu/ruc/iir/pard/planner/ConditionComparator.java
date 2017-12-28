package cn.edu.ruc.iir.pard.planner;

import cn.edu.ruc.iir.pard.sql.tree.BooleanLiteral;
import cn.edu.ruc.iir.pard.sql.tree.CharLiteral;
import cn.edu.ruc.iir.pard.sql.tree.DoubleLiteral;
import cn.edu.ruc.iir.pard.sql.tree.Literal;
import cn.edu.ruc.iir.pard.sql.tree.LongLiteral;
import cn.edu.ruc.iir.pard.sql.tree.NullLiteral;
import cn.edu.ruc.iir.pard.sql.tree.StringLiteral;

public class ConditionComparator
{
    public Comparable parseFromLiteral(Literal literal)
    {
        if (literal instanceof LongLiteral) {
            return Long.parseLong(literal.toString());
        }
        else
            if (literal instanceof DoubleLiteral) {
             // check literal type
            }
            else
                if (literal instanceof BooleanLiteral) {
                 // check literal type
                }
                else
                    if (literal instanceof CharLiteral) {
                     // check literal type
                    }
                    else
                        if (literal instanceof NullLiteral) {
                         // check literal type
                        }
                        else
                            if (literal instanceof StringLiteral) {
                             // check literal type
                            }
        return null;
    }
}
