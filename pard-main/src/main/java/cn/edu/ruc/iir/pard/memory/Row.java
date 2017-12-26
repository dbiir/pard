package cn.edu.ruc.iir.pard.memory;

/**
 * pard
 *
 * @author guodong
 */
public class Row
{
    private final byte[] content;
    private final int[] offsets;

    public Row(byte[] content, int[] offsets)
    {
        this.content = content;
        this.offsets = offsets;
    }
}
