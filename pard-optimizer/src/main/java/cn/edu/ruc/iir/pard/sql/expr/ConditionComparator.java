package cn.edu.ruc.iir.pard.sql.expr;

import cn.edu.ruc.iir.pard.commons.utils.DataType.DataTypeInt;
import cn.edu.ruc.iir.pard.sql.tree.BooleanLiteral;
import cn.edu.ruc.iir.pard.sql.tree.CharLiteral;
import cn.edu.ruc.iir.pard.sql.tree.DoubleLiteral;
import cn.edu.ruc.iir.pard.sql.tree.Literal;
import cn.edu.ruc.iir.pard.sql.tree.LongLiteral;
import cn.edu.ruc.iir.pard.sql.tree.NullLiteral;
import cn.edu.ruc.iir.pard.sql.tree.StringLiteral;

public class ConditionComparator
{
    private ConditionComparator()
    {}

    public static Comparable parseFromString(int dataType, String value)
    {
        switch(dataType) {
            case DataTypeInt.SMALLINT:
            case DataTypeInt.BIGINT:
            case DataTypeInt.INT:
                return Long.parseLong(value);
            case DataTypeInt.FLOAT:
            case DataTypeInt.DOUBLE:
                return Double.parseDouble(value);
            case DataTypeInt.TEXT:
            case DataTypeInt.CHAR:
            case DataTypeInt.VARCHAR:
                return value;
            case DataTypeInt.TIME:
            case DataTypeInt.DATE:
            case DataTypeInt.TIMESTAMP:
                return value;
        }
        return value;
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
}
