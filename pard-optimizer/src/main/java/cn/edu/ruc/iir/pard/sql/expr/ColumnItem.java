package cn.edu.ruc.iir.pard.sql.expr;

import cn.edu.ruc.iir.pard.sql.tree.DereferenceExpression;
import cn.edu.ruc.iir.pard.sql.tree.Expression;
import cn.edu.ruc.iir.pard.sql.tree.Identifier;

import java.util.HashMap;
import java.util.Map;

public class ColumnItem
        extends Item
{
    /**
     *
     */
    private static final long serialVersionUID = 1L;
    private final String tableName;
    private final String columnName;
    private int dataType;
    private static ThreadLocal<Map<String, String>> col2tblMap = new ThreadLocal<>();
    public static Map<String, String> getCol2TblMap()
    {
        Map<String, String> map = col2tblMap.get();
        if (map == null) {
            map = new HashMap<String, String>();
            col2tblMap.set(map);
        }
        return map;
    }
    public static void clearCol2TblMap()
    {
        col2tblMap.remove();
    }
    public ColumnItem(ColumnItem ci)
    {
        super();
        this.tableName = ci.tableName;
        this.columnName = ci.columnName;
        this.dataType = ci.dataType;
        this.expression = ci.expression;
    }
    public ColumnItem(String tableName, String columnName, int dataType)
    {
        super();
        this.tableName = tableName == null ? null : tableName.toLowerCase();
        this.columnName = columnName.toLowerCase();
        this.dataType = dataType;
    }

    public String getTableName()
    {
        return tableName;
    }

    public String getColumnName()
    {
        return columnName;
    }

    public int getDataType()
    {
        return dataType;
    }

    @Override
    public String toString()
    {
        if (tableName == null || tableName.isEmpty()) {
            return columnName;
        }
        else {
            return tableName + "." + columnName;
        }
    }
    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((columnName == null) ? 0 : columnName.hashCode());
        //result = prime * result + dataType;
        result = prime * result + ((tableName == null) ? 0 : tableName.hashCode());
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
        ColumnItem other = (ColumnItem) obj;
        if (columnName == null) {
            if (other.columnName != null) {
                return false;
            }
        }
        else if (!columnName.equals(other.columnName)) {
            return false;
        }
        //if (dataType != other.dataType) {
          //  return false;
        //}
        if (tableName == null) {
            if (other.tableName != null) {
                return false;
            }
        }
        else if (!tableName.equals(other.tableName)) {
            return false;
        }
        return true;
    }
    @Override
    public Expression toExpression()
    {
        if (expression != null) {
            return expression;
        }
        else if (tableName == null) {
            return new Identifier(this.columnName);
        }
        else {
            return new DereferenceExpression(new Identifier(tableName), new Identifier(this.columnName));
        }
    }
}
