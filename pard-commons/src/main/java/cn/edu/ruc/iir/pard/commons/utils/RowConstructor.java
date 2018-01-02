package cn.edu.ruc.iir.pard.commons.utils;

import cn.edu.ruc.iir.pard.commons.memory.Row;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * pard
 *
 * @author guodong
 */
public class RowConstructor
{
    private Row row;
    private ByteBuffer byteBuffer = ByteBuffer.allocate(1024 * 10);
    private List<Integer> offsets = new ArrayList<>();

    public void appendShort(short value)
    {
        byteBuffer.putShort(value);
        offsets.add(byteBuffer.arrayOffset());
    }

    public void appendInt(int value)
    {
        byteBuffer.putInt(value);
        offsets.add(byteBuffer.arrayOffset());
    }

    public void appendFloat(float value)
    {
        byteBuffer.putFloat(value);
        offsets.add(byteBuffer.arrayOffset());
    }

    public void appendDouble(double value)
    {
        byteBuffer.putDouble(value);
        offsets.add(byteBuffer.arrayOffset());
    }

    public void appendString(String value)
    {
        byteBuffer.put(value.getBytes());
        offsets.add(byteBuffer.arrayOffset());
    }

    public Row build()
    {
        return new Row(byteBuffer.array(), offsets.stream().mapToInt(i->i).toArray());
    }

    public static String printRow(Row row, List<DataType> dataTypes)
    {
        StringBuilder sb = new StringBuilder();
        ByteBuffer byteBuffer = ByteBuffer.wrap(row.getContent());
        int[] offsets = row.getOffsets();
        for (int i = 0; i < offsets.length; i++) {
            DataType dataType = dataTypes.get(i);
            if (dataType == DataType.INT) {
                sb.append(byteBuffer.getInt()).append(", ");
            }
            if (dataType == DataType.CHAR || dataType == DataType.VARCHAR) {
                int len = dataType.getLength();
                byte[] temp = new byte[len];
                byteBuffer.get(temp);
                sb.append(new String(temp)).append(", ");
            }
            if (dataType == DataType.BIGINT) {
                sb.append(byteBuffer.getLong()).append(", ");
            }
            if (dataType == DataType.FLOAT) {
                sb.append(byteBuffer.getFloat()).append(", ");
            }
            // todo add more types
        }
        return sb.toString();
    }
}
