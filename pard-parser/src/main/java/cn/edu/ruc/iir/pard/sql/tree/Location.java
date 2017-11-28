package cn.edu.ruc.iir.pard.sql.tree;

/**
 * pard
 *
 * @author guodong
 */
public class Location
{
    private final int line;
    private final int charPosInLine;

    public Location(int line, int charPosInLine)
    {
        this.line = line;
        this.charPosInLine = charPosInLine;
    }

    public int getLine()
    {
        return line;
    }

    public int getCharPosInLine()
    {
        return charPosInLine;
    }
}
