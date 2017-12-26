package cn.edu.ruc.iir.pard.connector.postgresql;

import cn.edu.ruc.iir.pard.scheduler.CreateSchemaTask;
import org.testng.annotations.Test;

/**
 * pard
 *
 * @author guodong
 */
public class TestConnector
{
    @Test
    public void testCreateSchema()
    {
        CreateSchemaTask task = new CreateSchemaTask("test", false);
    }

    @Test
    public void testCreateTable()
    {}

    @Test
    public void testInsert()
    {}
}
