package cn.edu.ruc.iir.pard.connector.postgresql;

import cn.edu.ruc.iir.pard.catalog.Column;
import cn.edu.ruc.iir.pard.catalog.DataType;
import cn.edu.ruc.iir.pard.commons.config.PardUserConfiguration;
import cn.edu.ruc.iir.pard.commons.utils.PardResultSet;
import cn.edu.ruc.iir.pard.executor.connector.CreateSchemaTask;
import cn.edu.ruc.iir.pard.executor.connector.CreateTableTask;
import cn.edu.ruc.iir.pard.executor.connector.DropSchemaTask;
import cn.edu.ruc.iir.pard.executor.connector.DropTableTask;
import cn.edu.ruc.iir.pard.executor.connector.InsertIntoTask;
import cn.edu.ruc.iir.pard.executor.connector.node.FilterNode;
import cn.edu.ruc.iir.pard.executor.connector.node.LimitNode;
import cn.edu.ruc.iir.pard.executor.connector.node.OutputNode;
import cn.edu.ruc.iir.pard.executor.connector.node.ProjectNode;
import cn.edu.ruc.iir.pard.executor.connector.node.SortNode;
import cn.edu.ruc.iir.pard.executor.connector.node.TableScanNode;
import cn.edu.ruc.iir.pard.sql.tree.ComparisonExpression;
import cn.edu.ruc.iir.pard.sql.tree.ComparisonExpressionType;
import cn.edu.ruc.iir.pard.sql.tree.Expression;
import cn.edu.ruc.iir.pard.sql.tree.Identifier;
import cn.edu.ruc.iir.pard.sql.tree.LogicalBinaryExpression;
import cn.edu.ruc.iir.pard.sql.tree.LongLiteral;
import cn.edu.ruc.iir.pard.sql.tree.StringLiteral;
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
        String[] tuple1 = new String[2];
        String[] tuple2 = new String[2];
        String[] tuple3 = new String[2];
        String[] tuple4 = new String[2];
        String[][] values = new String[4][2];
        tuple1[0] = "tomcat";
        tuple1[1] = "001";
        tuple2[0] = "jerrymouse";
        tuple2[1] = "002";
        tuple3[0] = "spikebulldog";
        tuple3[1] = "003";
        tuple4[0] = "tykebulldog";
        tuple4[1] = "004";
        values[0] = tuple1;
        values[1] = tuple2;
        values[2] = tuple3;
        values[3] = tuple4;
        /*
        for(String [] str : values) {
            for(String s : str) {
                System.out.print(s);
                System.out.print("\t");
            }
            System.out.println();
        }
        */
        InsertIntoTask task = new InsertIntoTask("pardschema", "table1", columns, values);
        PardResultSet resultSet = pConn.execute(task);
        System.out.println(resultSet.getStatus().toString());
        System.out.println("Added Number: " + pConn.getChNum());
    }

    @Test
    public void testQuery()
    {
//        final PostgresConnector pConn = PostgresConnector.INSTANCE();
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

        Identifier name = new Identifier("name");
        Identifier id = new Identifier("id");
        ComparisonExpression nameEqTomcatExpr = new ComparisonExpression(
                ComparisonExpressionType.EQUAL,
                name,
                new StringLiteral("tomcat"));
        ComparisonExpression idLT5Expr = new ComparisonExpression(
                ComparisonExpressionType.LESS_THAN,
                id,
                new LongLiteral("5"));
        Expression expression = new LogicalBinaryExpression(
                LogicalBinaryExpression.Type.AND,
                nameEqTomcatExpr,
                idLT5Expr);

        // SELECT name, id FROM pardschema.table1 WHERE name='tomcat' AND id<5 ORDER BY id LIMIT 8;
        OutputNode outputNode = new OutputNode();
        LimitNode limitNode = new LimitNode(3);
        SortNode sortNode = new SortNode();
        sortNode.addSort(col1, true);
        ProjectNode projectNode = new ProjectNode(columns);
        FilterNode filterNode = new FilterNode(expression);
        //FilterNode filterNode = new FilterNode(idLT5Expr);
        TableScanNode tableScanNode = new TableScanNode("pardschema", "table1");
        /*
        System.out.println("IN TEST");
        System.out.println(outputNode);
        System.out.println(limitNode);
        System.out.println(sortNode);
        System.out.println(projectNode);
        System.out.println(filterNode);
        System.out.println(tableScanNode);
        System.out.println("IN TEST");
        */
        // LIMIT -> SORT -> PROJECT -> FILTER -> SCAN
        outputNode.setChildren(limitNode, true);
        limitNode.setChildren(sortNode, true);
        sortNode.setChildren(projectNode, true);
        projectNode.setChildren(filterNode, true);
        //sortNode.setChildren(filterNode,true);
        filterNode.setChildren(tableScanNode, true);
        System.out.println(outputNode);

//        QueryTask task = new QueryTask(outputNode);
//        PardResultSet resultSet = pConn.execute(task);
//        System.out.println(resultSet.getNext().getRowSize());
        // todo print out resultSet and it satisfies the actual result
    }
}
