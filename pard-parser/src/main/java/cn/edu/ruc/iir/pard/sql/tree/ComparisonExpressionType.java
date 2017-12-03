package cn.edu.ruc.iir.pard.sql.tree;

/**
 * pard
 *
 * @author guodong
 */
public enum ComparisonExpressionType
{
    EQUAL("="),
    NOT_EQUAL("<>"),
    LESS_THAN("<"),
    LESS_THAN_OR_EQUAL("<="),
    GREATER_THAN(">"),
    GREATER_THAN_OR_EQUAL(">="),
    IS_DISTINCT_FROM("IS DISTINCT FROM");

    private final String value;

    ComparisonExpressionType(String value)
    {
        this.value = value;
    }

    public String getValue()
    {
        return value;
    }

    public ComparisonExpressionType flip()
    {
        switch (this) {
            case EQUAL:
                return EQUAL;
            case NOT_EQUAL:
                return NOT_EQUAL;
            case LESS_THAN:
                return GREATER_THAN;
            case LESS_THAN_OR_EQUAL:
                return GREATER_THAN_OR_EQUAL;
            case GREATER_THAN:
                return LESS_THAN;
            case GREATER_THAN_OR_EQUAL:
                return LESS_THAN_OR_EQUAL;
            case IS_DISTINCT_FROM:
                return IS_DISTINCT_FROM;
            default:
                throw new IllegalArgumentException("Unsupported comparison: " + this);
        }
    }

    public ComparisonExpressionType negate()
    {
        switch (this) {
            case EQUAL:
                return NOT_EQUAL;
            case NOT_EQUAL:
                return EQUAL;
            case LESS_THAN:
                return GREATER_THAN_OR_EQUAL;
            case LESS_THAN_OR_EQUAL:
                return GREATER_THAN;
            case GREATER_THAN:
                return LESS_THAN_OR_EQUAL;
            case GREATER_THAN_OR_EQUAL:
                return LESS_THAN;
            default:
                throw new IllegalArgumentException("Unsupported comparison: " + this);
        }
    }
}
