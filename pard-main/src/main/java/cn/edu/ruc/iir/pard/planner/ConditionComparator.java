package cn.edu.ruc.iir.pard.planner;

import cn.edu.ruc.iir.pard.catalog.Condition;
import cn.edu.ruc.iir.pard.catalog.GddUtil;
import cn.edu.ruc.iir.pard.commons.utils.DataType.DataTypeInt;
import cn.edu.ruc.iir.pard.planner.ErrorMessage.ErrCode;
import cn.edu.ruc.iir.pard.semantic.SemanticException;
import cn.edu.ruc.iir.pard.sql.tree.BooleanLiteral;
import cn.edu.ruc.iir.pard.sql.tree.CharLiteral;
import cn.edu.ruc.iir.pard.sql.tree.DoubleLiteral;
import cn.edu.ruc.iir.pard.sql.tree.Literal;
import cn.edu.ruc.iir.pard.sql.tree.LongLiteral;
import cn.edu.ruc.iir.pard.sql.tree.NullLiteral;
import cn.edu.ruc.iir.pard.sql.tree.StringLiteral;

//import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ConditionComparator
{
    private ConditionComparator()
    {}

    public static boolean matchString(List<Condition> conditions, Map<String, String> value)
    {
        for (Condition cond : conditions) {
            Comparable condCmp = parseFromString(cond.getDataType(), cond.getValue());
//            int vIndex = Arrays.binarySearch(names, cond.getColumnName());
//            if (vIndex < 0 || vIndex >= values.length) {
                // column not found
//                continue;
//            }
//            String str = values[vIndex];
            String str = value.get(cond.getColumnName());
            if (str == null) {
                throw new SemanticException(ErrCode.MissingPartitionColumnsWhenInsert, cond.getColumnName());
            }
            Comparable valueCmp = parseFromString(cond.getDataType(), str);
            try {
                if (!compare(cond, condCmp, valueCmp)) {
                    return false;
                }
            }
            catch (SemanticException e) {
                e.printStackTrace();
                return false;
            }
        }
        return true;
    }

    public static boolean matchLiteral(List<Condition> conditions, Map<String, Literal> valueMap)
    {
        for (Condition cond : conditions) {
            Comparable condcmp = parseFromString(cond.getDataType(), cond.getValue());
            Literal literal = valueMap.get(cond.getColumnName());
            if (literal == null) {
                throw new SemanticException(ErrCode.MissingPartitionColumnsWhenInsert, cond.getColumnName());
            }
            Comparable valueCmp = parseFromLiteral(literal);
            if (literal instanceof NullLiteral) {
                if (cond.getCompareType() != GddUtil.compareLESS && cond.getCompareType() != GddUtil.compareLESSEQUAL) {
                    if (!"null".equals(cond.getColumnName())) {
                        return false;
                    }
                }
            }
            try {
                if (!compare(cond, condcmp, valueCmp)) {
                    return false;
                }
            }
            catch (SemanticException e) {
                e.printStackTrace();
                return false;
            }
        }
        return true;
    }

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

    private static boolean compare(Condition cond, Comparable condCmp, Comparable valueCmp) throws SemanticException
    {
        switch (cond.getCompareType()) {
            case GddUtil.compareEQUAL:
                if (!valueCmp.equals(condCmp)) {
                    return false;
                }
                break;
            case GddUtil.compareGREAT:
                if (!(valueCmp.compareTo(condCmp) > 0)) {
                    return false;
                }
                break;
            case GddUtil.compareGREATEQUAL:
                if (!(valueCmp.compareTo(condCmp) >= 0)) {
                    return false;
                }
                break;
            case GddUtil.compareLESS:
                if (!(valueCmp.compareTo(condCmp) < 0)) {
                    return false;
                }
                break;
            case GddUtil.compareLESSEQUAL:
                if (!(valueCmp.compareTo(condCmp) <= 0)) {
                    return false;
                }
                break;
            case GddUtil.compareNOTEQUAL:
                if (!(valueCmp.compareTo(condCmp) != 0)) {
                    return false;
                }
                break;
            default:
                throw new SemanticException(ErrCode.UnkownCompareTypeWhenPartition);
        }
        return true;
    }
}
