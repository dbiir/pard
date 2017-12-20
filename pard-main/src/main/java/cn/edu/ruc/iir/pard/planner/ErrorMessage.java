package cn.edu.ruc.iir.pard.planner;

import java.util.HashMap;
import java.util.Map;

public class ErrorMessage
{
    private int errcode;
    private String errmsg;
    private static Map<Integer, String> template = null;
    private static ErrorMessage ok = throwMessage(ErrCode.OK);
    public static ErrorMessage throwMessage(int errorCode, Object...objects)
    {
        return new ErrorMessage(errorCode, objects);
    }
    public static ErrorMessage getOKMessage()
    {
        if (ok == null) {
            ok = throwMessage(ErrCode.OK);
        }
        return ok;
    }
    public static class ErrCode
    {
        public static final int OK = 1;
        public static final int ParseError = -10000;
        public static final int SchemaExsits = -10001;
        public static final int SchemaNotExsits = -10002;
        public static final int SchemaNotSpecified = -10003;
        public static final int TableExists = -10004;
        public static final int NotaColumnDefinition = -10005;
    }
    public static void init()
    {
        template = new HashMap<Integer, String>();
        template.put(ErrCode.ParseError, "parse error! it is not a %s!");
        template.put(ErrCode.SchemaExsits, "Schema %s  exsits!");
        template.put(ErrCode.SchemaNotExsits, "Schema %s  does not exsit!");
        template.put(ErrCode.SchemaNotSpecified, "Schema not specified! Please use 'Use [Schema]' statement to specify.");
        template.put(ErrCode.TableExists, "Table %s is already exsits in Schema %s!");
        template.put(ErrCode.NotaColumnDefinition, "string %s is not a column definition!");
        template.put(ErrCode.OK, "success");
    }
    public Map<Integer, String> getTemplate()
    {
        if (template == null) {
            init();
        }
        return template;
    }
    public ErrorMessage()
    {}
    public ErrorMessage(String errMsg, int errcode)
    {
        this.errcode = errcode;
        this.errmsg = errMsg;
    }
    public ErrorMessage(int errorCode, Object...objects)
    {
        String tmp = template.get(errorCode);
        this.errcode = errorCode;
        this.errmsg = String.format(tmp, objects);
    }
    public int getErrcode()
    {
        return errcode;
    }
    public void setErrcode(int errcode)
    {
        this.errcode = errcode;
    }
    public String getErrmsg()
    {
        return errmsg;
    }
    public void setErrmsg(String errmsg)
    {
        this.errmsg = errmsg;
    }
    public void setMsg(int errorCode, Object...objects)
    {
        String tmp = template.get(errorCode);
        this.errcode = errorCode;
        this.errmsg = String.format(tmp, objects);
    }
}
