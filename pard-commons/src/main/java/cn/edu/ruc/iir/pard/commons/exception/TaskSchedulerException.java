package cn.edu.ruc.iir.pard.commons.exception;

public class TaskSchedulerException
        extends PardException
{
    /**
     *
     */
    private static final long serialVersionUID = 1L;

    public TaskSchedulerException()
    {
        super();
    }

    public TaskSchedulerException(ErrorMessage msg)
    {
        super(msg);
    }

    public TaskSchedulerException(int errorCode, Object... objs)
    {
        super(errorCode, objs);
    }
}
