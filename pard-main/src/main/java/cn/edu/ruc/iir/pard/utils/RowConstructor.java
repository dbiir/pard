package cn.edu.ruc.iir.pard.utils;

import cn.edu.ruc.iir.pard.memory.Row;

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
}
