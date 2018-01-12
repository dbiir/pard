package cn.edu.ruc.iir.pard.executor.connector;

import org.testng.annotations.Test;

/**
 * pard
 *
 * @author guodong
 */
public class PardResultSetTest
{
    @Test
    public void testRS()
    {
        PardResultSet resultSet = new PardResultSet();
    }

    @Test
    public void testEnum()
    {
        PardResultSet r = PardResultSet.okResultSet;
        if (r.getStatus() == PardResultSet.ResultStatus.OK) {
            System.out.println("Y");
        }
        else {
            System.out.println("N");
        }
    }
}
