package cn.edu.ruc.iir.pard.commons.utils;

import cn.edu.ruc.iir.pard.commons.memory.Row;

import java.io.UnsupportedEncodingException;
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
        offsets.add(byteBuffer.position());
    }

    public void appendInt(int value)
    {
        byteBuffer.putInt(value);
        offsets.add(byteBuffer.position());
    }

    public void appendFloat(float value)
    {
        byteBuffer.putFloat(value);
        offsets.add(byteBuffer.position());
    }

    public void appendDouble(double value)
    {
        byteBuffer.putDouble(value);
        offsets.add(byteBuffer.position());
    }

    public void appendString(String value)
    {
        byteBuffer.put(value.getBytes());
        offsets.add(byteBuffer.position());
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
            if (dataType.getType() == DataType.INT.getType()) {
                sb.append(byteBuffer.getInt()).append(", ");
            }
            if (dataType.getType() == DataType.CHAR.getType() || dataType.getType() == DataType.VARCHAR.getType()) {
                int len = 0;
                if (i == 0) {
                    len = offsets[i];
                }
                else {
                    len = offsets[i] - offsets[i - 1];
                }
                byte[] temp = new byte[len];
                byteBuffer.get(temp);
                try {
                    sb.append(new String(temp, "UTF-8")).append(", ");
                }
                catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                }
            }
            if (dataType.getType() == DataType.BIGINT.getType()) {
                sb.append(byteBuffer.getLong()).append(", ");
            }
            if (dataType.getType() == DataType.FLOAT.getType()) {
                sb.append(byteBuffer.getFloat()).append(", ");
            }
            // todo add more types
        }
        return sb.toString();
    }
}
