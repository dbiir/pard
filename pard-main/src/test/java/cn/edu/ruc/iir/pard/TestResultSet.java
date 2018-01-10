package cn.edu.ruc.iir.pard;

import org.testng.annotations.Test;

/**
 * pard
 *
 * @author guodong
 */
public class TestResultSet
{
    @Test
    public void stringReplaceTest()
    {
        String s = "\"sd'sd\"";
        System.out.println(s);
        System.out.println(s.replaceAll("\"|'", ""));
    }
}
