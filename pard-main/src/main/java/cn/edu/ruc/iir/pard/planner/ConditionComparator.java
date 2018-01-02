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

import java.util.List;
import java.util.Map;

public class ConditionComparator
{
    private ConditionComparator()
    {}
    public static boolean match(List<Condition> conditions, Map<String, Literal> valueMap)
    {
        for (Condition cond : conditions) {
            Comparable condcmp = parseFromString(cond.getDataType(), cond.getValue());
            Literal literal = valueMap.get(cond.getColumnName());
            if (literal == null) {
                throw new SemanticException(ErrCode.MissingPartitionColumnsWhenInsert, cond.getColumnName());
            }
            Comparable condcmp2 = parseFromLiteral(literal);
            if (literal instanceof NullLiteral) {
                if (cond.getCompareType() != GddUtil.compareLESS && cond.getCompareType() != GddUtil.compareLESSEQUAL) {
                    if (!"null".equals(cond.getColumnName())) {
                        return false;
                    }
                }
            }
            try {
                switch (cond.getCompareType()) {
                    case GddUtil.compareEQUAL:
                        if (!condcmp2.equals(condcmp)) {
                            return false;
                        }
                        break;
                    case GddUtil.compareGREAT:
                        if (!(condcmp2.compareTo(condcmp) > 0)) {
                            return false;
                        }
                        break;
                    case GddUtil.compareGREATEQUAL:
                        if (!(condcmp2.compareTo(condcmp) >= 0)) {
                            return false;
                        }
                        break;
                    case GddUtil.compareLESS:
                        if (!(condcmp2.compareTo(condcmp) < 0)) {
                            return false;
                        }
                        break;
                    case GddUtil.compareLESSEQUAL:
                        if (!(condcmp2.compareTo(condcmp) <= 0)) {
                            return false;
                        }
                        break;
                    case GddUtil.compareNOTEQUAL:
                        if (!(condcmp2.compareTo(condcmp) != 0)) {
                            return false;
                        }
                        break;
                    default:
                        throw new SemanticException(ErrCode.UnkownCompareTypeWhenPartition);
                }
            }
            catch (ClassCastException e) {
                e.printStackTrace();
                throw new SemanticException(ErrCode.ValuesTypeNotMatch, literal.toString());
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
}
