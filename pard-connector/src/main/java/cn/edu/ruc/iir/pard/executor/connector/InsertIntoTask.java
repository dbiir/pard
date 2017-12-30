package cn.edu.ruc.iir.pard.executor.connector;

import cn.edu.ruc.iir.pard.catalog.Column;

import java.util.List;

/**
 * pard
 *
 * @author guodong
 */
public class InsertIntoTask
        extends Task
{
    private final List<Column> columns;
    private final String[][] values;

    public InsertIntoTask(List<Column> columns, String[][] values)
    {
        this.columns = columns;
        this.values = values;
    }

    public List<Column> getColumns()
    {
        return columns;
    }

    public String[][] getValues()
    {
        return values;
    }
}
