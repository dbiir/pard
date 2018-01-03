package cn.edu.ruc.iir.pard;

import cn.edu.ruc.iir.pard.commons.memory.Block;
import cn.edu.ruc.iir.pard.commons.memory.Row;
import cn.edu.ruc.iir.pard.commons.utils.DataType;
import cn.edu.ruc.iir.pard.commons.utils.PardResultSet;
import cn.edu.ruc.iir.pard.commons.utils.RowConstructor;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * pard
 *
 * @author guodong
 */
public class TestResultSet
{
    @Test
    public void testResultSet()
    {
        PardResultSet resultSet = new PardResultSet(PardResultSet.ResultStatus.OK);
        List<String> columnNames = new ArrayList<>();
        columnNames.add("name");
        columnNames.add("id");
        columnNames.add("age");
        List<DataType> dataTypes = new ArrayList<>();
        dataTypes.add(new DataType(DataType.CHAR.getType(), 20));
        dataTypes.add(new DataType(DataType.VARCHAR.getType(), 20));
        dataTypes.add(new DataType(DataType.INT.getType(), 10));
        Block block = new Block(columnNames, dataTypes, 1024 * 1024);

        for (int i = 0; i < 1000; i++) {
            RowConstructor rowConstructor = new RowConstructor();
            rowConstructor.appendString("ali");
            rowConstructor.appendString(String.valueOf(i));
            rowConstructor.appendInt(i + 10);
            Row row = rowConstructor.build();
            block.addRow(row);
        }

        resultSet.addBlock(block);

        if (resultSet.getStatus() == PardResultSet.ResultStatus.OK || resultSet.getStatus() == PardResultSet.ResultStatus.EOR) {
            System.out.println(resultSet.toString());
            List<String> colNames;
            List<DataType> colTypes;
            while (resultSet.hasNext()) {
                Block b = resultSet.getNext();
                colNames = b.getColumnNames();
                String header = Arrays.toString(colNames.toArray());
                System.out.println(header);
                for (int i = 0; i < header.length(); i++) {
                    System.out.print("-");
                }
                System.out.print("\n");
                colTypes = b.getColumnTypes();
                while (b.hasNext()) {
                    Row row = b.getNext();
                    System.out.println(RowConstructor.printRow(row, colTypes));
                }
            }
        }
        else {
            System.out.println(resultSet.getStatus().toString());
        }
        System.out.flush();
    }
}
