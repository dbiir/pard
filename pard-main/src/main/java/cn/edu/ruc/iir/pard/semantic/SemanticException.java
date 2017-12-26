package cn.edu.ruc.iir.pard.semantic;

import cn.edu.ruc.iir.pard.planner.ErrorMessage;

/**
 * SemanticException
 * 用于处理语义解析时的异常
 *
 * @author hagen
 * */
public class SemanticException
        extends RuntimeException
{
    /**
     *
     */
    private static final long serialVersionUID = -8125939273116913981L;
    private final ErrorMessage semanticErrorMessage;

    public SemanticException(int errorCode, Object...objs)
    {
        semanticErrorMessage = ErrorMessage.throwMessage(errorCode, objs);
    }
    public SemanticException(ErrorMessage msg)
    {
        semanticErrorMessage = msg;
    }
    public ErrorMessage getSemanticErrorMessage()
    {
        return semanticErrorMessage;
    }
}
