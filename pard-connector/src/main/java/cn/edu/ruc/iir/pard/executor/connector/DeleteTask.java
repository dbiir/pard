package cn.edu.ruc.iir.pard.executor.connector;

import cn.edu.ruc.iir.pard.sql.tree.Expression;

/**
 * pard
 *
 * @author guodong
 */
public class DeleteTask
        extends Task
{
    private static final long serialVersionUID = -274611652485202377L;

    private final String schema;
    private final String table;
    private final Expression expression;

    public DeleteTask(String schema, String table, Expression expression)
    {
        this(schema, table, expression, null);
    }

    public DeleteTask(String schema, String table, Expression expression, String site)
    {
        super(site);
        this.schema = schema;
        this.table = table;
        this.expression = expression;
    }

    public String getSchema()
    {
        return schema;
    }

    public String getTable()
    {
        return table;
    }

    public Expression getExpression()
    {
        return expression;
    }
}
