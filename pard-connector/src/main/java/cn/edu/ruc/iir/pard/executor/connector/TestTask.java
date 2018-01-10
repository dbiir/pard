package cn.edu.ruc.iir.pard.executor.connector;

/**
 * pard
 *
 * @author guodong
 */
public class TestTask
        extends Task
{
    private static final long serialVersionUID = 7743124335801967489L;

    public TestTask(String site)
    {
        super(site);
    }

    @Override
    public String toString()
    {
        return "This is a test task";
    }
}
