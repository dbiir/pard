package cn.edu.ruc.iir.pard.sql.tree;

import java.io.Serializable;

/**
 * pard
 *
 * @author guodong
 */
public class Location
        implements Serializable
{
    private static final long serialVersionUID = -3648104808846800441L;
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
