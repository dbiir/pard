package cn.edu.ruc.iir.pard.connector.postgresql;

import org.testng.annotations.Test;

/**
 * pard
 *
 * @author guodong
 */
public class TestConnector
{
//    private final PostgresConnector pConn = new PostgresConnector();

    @Test
    public void testCreateSchema()
    {
//        CreateSchemaTask task = new CreateSchemaTask("test", false, "-1");
//        pConn.execute(task);
    }

    @Test
    public void testCreateTable()
    {
//        ArrayList<ColumnDefinition> columnDefinitions = new ArrayList<ColumnDefinition>();
//        Identifier id1 = new Identifier(null, "ID", false);
//        Identifier id2 = new Identifier(null, "birthdate", false);
//        Identifier id3 = new Identifier(null, "salary", false);
//        ColumnDefinition cd1 = new ColumnDefinition(id1, "int", true);
//        ColumnDefinition cd2 = new ColumnDefinition(id2, "char(16)", false);
//        ColumnDefinition cd3 = new ColumnDefinition(id3, "varchar(20)", false);
//        columnDefinitions.add(cd1);
//        columnDefinitions.add(cd2);
//        columnDefinitions.add(cd3);
//        CreateTableTask task = new CreateTableTask("testschema", "table1", false, columnDefinitions, "-1");
//        pConn.execute(task);
    }

    @Test
    public void testInsert()
    {}

    @Test
    public void testSelect()
    {}
}
