package cn.edu.ruc.iir.pard.connector.postgresql;

import cn.edu.ruc.iir.pard.catalog.Column;
import cn.edu.ruc.iir.pard.catalog.DataType;
import cn.edu.ruc.iir.pard.commons.config.PardUserConfiguration;
import cn.edu.ruc.iir.pard.commons.utils.PardResultSet;
import cn.edu.ruc.iir.pard.executor.connector.CreateSchemaTask;
import cn.edu.ruc.iir.pard.executor.connector.CreateTableTask;
import cn.edu.ruc.iir.pard.executor.connector.DropSchemaTask;
import cn.edu.ruc.iir.pard.executor.connector.DropTableTask;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * pard
 *
 * @author guodong
 */
public class TestConnector
{
    private final PardUserConfiguration configuration = PardUserConfiguration.INSTANCE();

    @BeforeTest
    public void init()
    {
        configuration.init("../pard-main/etc/pard.properties");
    }

    @Test
    public void testCreateSchema()
    {
        final PostgresConnector pConn = PostgresConnector.INSTANCE();
        CreateSchemaTask task = new CreateSchemaTask("pardschema", false);
        PardResultSet resultSet = pConn.execute(task);
        System.out.println(resultSet.getStatus().toString());
    }

    @Test
    public void testCreateTable()
    {
        final PostgresConnector pConn = PostgresConnector.INSTANCE();
        List<Column> columns = new ArrayList<>();
        Column col0 = new Column();
        col0.setDataType(DataType.CHAR.getType());
        col0.setLen(20);
        col0.setColumnName("name");
        Column col1 = new Column();
        col1.setDataType(DataType.INT.getType());
        col1.setColumnName("id");
        col1.setKey(1);
        columns.add(col0);
        columns.add(col1);
        CreateTableTask task = new CreateTableTask("pardschema", "table1", false, columns);
        PardResultSet resultSet = pConn.execute(task);
        System.out.println(resultSet.getStatus().toString());
    }

    @Test
    public void testDropSchema()
    {
        final PostgresConnector pConn = PostgresConnector.INSTANCE();
        DropSchemaTask task = new DropSchemaTask("pardschema", false);
        PardResultSet resultSet = pConn.execute(task);
        System.out.println(resultSet.getStatus().toString());
    }

    @Test
    public void testDropTable()
    {
        final PostgresConnector pConn = PostgresConnector.INSTANCE();
        DropTableTask task = new DropTableTask("pardschema", "table1");
        PardResultSet resultSet = pConn.execute(task);
        System.out.println(resultSet.getStatus().toString());
    }

    @Test
    public void testInsert()
    {
        final PostgresConnector pConn = PostgresConnector.INSTANCE();
    }

    @Test
    public void testSelect()
    {
        final PostgresConnector pConn = PostgresConnector.INSTANCE();
    }
}
