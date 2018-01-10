package cn.edu.ruc.iir.pard.commons.memory;

import java.io.Serializable;

/**
 * pard
 *
 * @author guodong
 */
public class Row
        implements Serializable
{
    private static final long serialVersionUID = -8278491114129236654L;

    private final byte[] content;
    private final int[] offsets;

    public Row(byte[] content, int[] offsets)
    {
        this.content = content;
        this.offsets = offsets;
    }

    public byte[] getContent()
    {
        return content;
    }

    public int[] getOffsets()
    {
        return offsets;
    }

    public int getSize()
    {
        return content.length;
    }
}
