package cn.edu.ruc.iir.pard.commons.exception;

/**
 * SemanticException
 * 用于处理语义解析时的异常
 *
 * @author hagen
 * */
public class SemanticException
        extends PardException
{
    /**
     *
     */
    private static final long serialVersionUID = -8125939273116913981L;
    private final ErrorMessage semanticErrorMessage;

    public SemanticException()
    {
        semanticErrorMessage = null;
    }

    public SemanticException(int errorCode, Object...objs)
    {
        semanticErrorMessage = ErrorMessage.throwMessage(errorCode, objs);
        if (semanticErrorMessage.getException() != null) {
            this.initCause(semanticErrorMessage.getException());
        }
    }
    public SemanticException(ErrorMessage msg)
    {
        semanticErrorMessage = msg;
        if (semanticErrorMessage.getException() != null) {
            this.initCause(semanticErrorMessage.getException());
        }
    }
    public ErrorMessage getSemanticErrorMessage()
    {
        return semanticErrorMessage;
    }
}
