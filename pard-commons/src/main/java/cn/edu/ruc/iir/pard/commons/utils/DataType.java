package cn.edu.ruc.iir.pard.commons.utils;

import java.io.Serializable;

public class DataType
        implements Serializable
{
    private static final long serialVersionUID = 1673501329658076878L;

    public static class DataTypeInt
            implements Serializable
    {
        private static final long serialVersionUID = 3963008609942896618L;
        public static final int SMALLINT = 0x81;
        public static final int INT = 0x82;
        public static final int BIGINT = 0x83;
        public static final int FLOAT = 0x84;
        public static final int DOUBLE = 0x85;

        public static final int VARCHAR = 0x86;
        public static final int CHAR = 0x87;
        public static final int TEXT = 0x88;

        public static final int DATE = 0x89;
        public static final int TIME = 0x90;
        public static final int TIMESTAMP = 0x91;
    }
    public static final DataType SMALLINT = new DataType(DataTypeInt.SMALLINT, 2);
    public static final DataType INT = new DataType(DataTypeInt.INT, 4);
    public static final DataType BIGINT = new DataType(DataTypeInt.BIGINT, 8);
    public static final DataType FLOAT = new DataType(DataTypeInt.FLOAT, 4);
    public static final DataType DOUBLE = new DataType(DataTypeInt.DOUBLE, 8);

    public static final DataType VARCHAR = new DataType(DataTypeInt.VARCHAR, 255);
    public static final DataType CHAR = new DataType(DataTypeInt.CHAR, 255);
    public static final DataType TEXT = new DataType(DataTypeInt.TEXT, 0);

    public static final DataType DATE = new DataType(DataTypeInt.DATE, 4);
    public static final DataType TIME = new DataType(DataTypeInt.TIME, 8);
    public static final DataType TIMESTAMP = new DataType(DataTypeInt.TIMESTAMP, 8);
    //TODO: 检查用户输入长度和最大长度的关系，如果大于最大长度，该抛出异常
    public static DataType getDataType(String type)
    {
        String dt = null;
        int length = 0;
        int pos1 = type.indexOf("(");
        int pos2 = type.indexOf(")");
        if (pos1 > 0 && pos2 > 0) {
            length = Integer.parseInt(type.substring(pos1 + 1, pos2));
            dt = type.substring(0, pos1);
        }
        else {
            dt = type;
        }
        dt = dt.trim();
        if (dt.equalsIgnoreCase("smallint") || dt.equalsIgnoreCase("tinyint")) {
            return SMALLINT;
        }
        else if (dt.equalsIgnoreCase("int") || dt.equalsIgnoreCase("integer")) {
            return INT;
        }
        else if (dt.equalsIgnoreCase("bigint") || dt.equalsIgnoreCase("long")) {
            return BIGINT;
        }
        else if (dt.equalsIgnoreCase("float") || dt.equalsIgnoreCase("real")) {
            return FLOAT;
        }
        else if (dt.equalsIgnoreCase("double")) {
            return DOUBLE;
        }
        else if (dt.equalsIgnoreCase("varchar") || dt.equalsIgnoreCase("nvarchar")) {
            DataType datat = new DataType(VARCHAR);
            if (length < datat.length) {
                datat.length = length;
            }
            return datat;
        }
        else if (dt.equalsIgnoreCase("char")) {
            DataType datat = new DataType(CHAR);
            if (length < datat.length) {
                datat.length = length;
            }
            return datat;
        }
        else if (dt.equalsIgnoreCase("text")) {
            return TEXT;
        }
        else if (dt.equalsIgnoreCase("date")) {
            return DATE;
        }
        else if (dt.equalsIgnoreCase("time")) {
            return TIME;
        }
        else if (dt.equalsIgnoreCase("datetime") || dt.equalsIgnoreCase("timestamp")) {
            return TIMESTAMP;
        }
        return null;
    }

    private int type;
    private int length;
    public DataType(int type, int len)
    {
        this.type = type;
        this.length = len;
    }
    public DataType(DataType dt)
    {
        this.type = dt.type;
        this.length = dt.length;
    }
    public int getType()
    {
        return type;
    }
    public void setType(int type)
    {
        this.type = type;
    }
    public int getLength()
    {
        return length;
    }
    public void setLength(int length)
    {
        this.length = length;
    }
    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + length;
        result = prime * result + type;
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
        DataType other = (DataType) obj;
        if (length != other.length) {
            return false;
        }
        if (type != other.type) {
            return false;
        }
        return true;
    }
}
