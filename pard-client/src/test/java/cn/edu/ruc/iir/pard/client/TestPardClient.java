package cn.edu.ruc.iir.pard.client;

import cn.edu.ruc.iir.pard.commons.utils.DataType;
import cn.edu.ruc.iir.pard.commons.utils.RowConstructor;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

/**
 * pard
 *
 * @author guodong
 */
public class TestPardClient
{
    @Test
    private void testPrettyTable()
    {
        PrettyTable table = new PrettyTable("Firstname", "Lastname", "Email", "Phone");
        table.addRow("John", "Doe", "johndoe@nothing.com", "+2137999999");
        table.addRow("John", "Doe", "johndoe@nothing.com", "+2137999999");
        table.addRow("John", "Doe", "johndoe@nothing.com", "+2137999999");
        table.addRow("John", "Doe", "johndoe@nothing.com", "+2137999999");
        table.addRow("John", "Doe", "johndoe@nothing.com", "+2137999999");
        table.addRow("John", "Doe", "johndoe@nothing.com", "+2137999999");
        table.addRow("Jane", "Doe", "janedoe@nothin.com", "+2137999999");
        System.out.println(table);

        RowConstructor rowConstructor = new RowConstructor();
        rowConstructor.appendString("ssdd");
        rowConstructor.appendInt(122);
        rowConstructor.appendDouble(0.44d);
        System.out.println(RowConstructor.printRow(rowConstructor.build(),
                ImmutableList.of(DataType.VARCHAR.getType(), DataType.INT.getType(), DataType.DOUBLE.getType())));
    }

    @Test
    public void testClient()
    {
        String host = "10.77.40.31";
        int port = 11013;
        String[] args = {host, port + ""};
        PardClient.main(args);
    }
}
