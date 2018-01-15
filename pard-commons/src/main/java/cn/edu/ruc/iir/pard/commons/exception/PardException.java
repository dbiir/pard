package cn.edu.ruc.iir.pard.commons.exception;

/**
 * pard
 *
 * @author guodong
 */
public class PardException
        extends RuntimeException
{
        /**
        *
        */
    private static final long serialVersionUID = -8125939273116913981L;
    private final ErrorMessage pardErrorMessage;

    public PardException()
    {
        pardErrorMessage = null;
    }

    public PardException(int errorCode, Object... objs)
    {
        pardErrorMessage = ErrorMessage.throwMessage(errorCode, objs);
        if (pardErrorMessage.getException() != null) {
            this.initCause(pardErrorMessage.getException());
        }
    }

    public PardException(ErrorMessage msg)
    {
        pardErrorMessage = msg;
        if (pardErrorMessage.getException() != null) {
            this.initCause(pardErrorMessage.getException());
        }
    }

    public ErrorMessage getPardErrorMessage()
    {
        return pardErrorMessage;
    }
}
