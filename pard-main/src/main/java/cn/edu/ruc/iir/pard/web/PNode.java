package cn.edu.ruc.iir.pard.web;

public class PNode
{
    private String key;
    private String text;
    private String figure;
    private String fill;
    private int stepType;
    private String loc;
    public String getKey()
    {
        return key;
    }
    public void setKey(String key)
    {
        this.key = key;
    }
    public String getText()
    {
        return text;
    }
    public void setText(String text)
    {
        this.text = text;
    }
    public String getFigure()
    {
        return figure;
    }
    public void setFigure(String figure)
    {
        this.figure = figure;
    }
    public String getFill()
    {
        return fill;
    }
    public void setFill(String fill)
    {
        this.fill = fill;
    }
    public int getStepType()
    {
        return stepType;
    }
    public void setStepType(int stepType)
    {
        this.stepType = stepType;
    }
    public String getLoc()
    {
        return locx + " " + locy;
    }
    public void setLoc(String loc)
    {
        this.loc = loc;
    }
    int locx;
    int locy;
}
