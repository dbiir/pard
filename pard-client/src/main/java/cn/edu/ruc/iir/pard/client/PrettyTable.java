package cn.edu.ruc.iir.pard.client;

import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PrettyTable
{
    private List<String> headers = new ArrayList<>();
    private List<List<String>> data = new ArrayList<>();
    private List<Integer> colLength = new ArrayList<>();

    public PrettyTable(String... headers)
    {
        this.headers.addAll(Arrays.asList(headers));
    }

    public void addRow(String... row)
    {
        data.add(Arrays.asList(row));
    }

    private int getMaxSize(int column)
    {
        int maxSize = headers.get(column).length();
        for (List<String> row : data) {
            if (row.get(column).length() > maxSize) {
                maxSize = row.get(column).length();
            }
        }
        return maxSize;
    }

    private String formatRow(List<String> row)
    {
        StringBuilder result = new StringBuilder();
        result.append("|");
        for (int i = 0; i < row.size(); i++) {
            result.append(StringUtils.center(row.get(i), getMaxSize(i) + 2));
            result.append("|");
        }
        result.append("\n");
        return result.toString();
    }

    private String formatRule()
    {
        StringBuilder result = new StringBuilder();
        result.append("+");
        for (int i = 0; i < headers.size(); i++) {
            for (int j = 0; j < getMaxSize(i) + 2; j++) {
                result.append("-");
            }
            result.append("+");
        }
        result.append("\n");
        return result.toString();
    }

    public String toStringOld()
    {
        StringBuilder result = new StringBuilder();
        result.append(formatRule());
        result.append(formatRow(headers));
        result.append(formatRule());
        for (List<String> row : data) {
            result.append(formatRow(row));
        }
        result.append(formatRule());
        return result.toString();
    }

    public int rowSize()
    {
        return data.size();
    }

    public String printHeader()
    {
        StringBuilder result = new StringBuilder();
        result.append(formatRule());
        result.append(formatRow(headers));
        result.append(formatRule());
        return result.toString();
    }

    public String printEnd()
    {
        StringBuilder result = new StringBuilder();
        result.append(formatRule());
        return result.toString();
    }

    public void printLargeDataSets()
    {
        System.out.println(printHeader());
        StringBuilder result = new StringBuilder();
        int count = 0;
        for (List<String> row : data) {
            result.append(formatRow(row));
            count++;
            if (count == 5000) {
                System.out.println(result.toString());
                result.delete(0, result.length());
                count = 0;
            }
        }
        System.out.println(printEnd());
    }

    public void printLargeDataSetsOneByOne()
    {
        System.out.println(printHeader());
        for (List<String> row : data) {
            System.out.println(formatRow(row));
        }
        System.out.println(printEnd());
    }

    private int getMaxSizeForCol(int column)
    {
        int maxSize = headers.get(column).length();
        for (List<String> row : data) {
            if (row.get(column).length() > maxSize) {
                maxSize = row.get(column).length();
            }
        }
        return maxSize;
    }

    private String formatRuleNew()
    {
        StringBuilder result = new StringBuilder();
        result.append("+");
        for (int i = 0; i < headers.size(); i++) {
            for (int j = 0; j < getMaxSize(i) + 2; j++) {
                result.append("-");
            }
            colLength.add(new Integer(getMaxSizeForCol(i)));
            result.append("+");
        }
        result.append("\n");
        return result.toString();
    }

    private String formatRowNew(List<String> row)
    {
        StringBuilder result = new StringBuilder();
        result.append("|");
        for (int i = 0; i < row.size(); i++) {
            result.append(StringUtils.center(row.get(i), colLength.get(i).intValue() + 2));
            //System.out.println(colLength.get(i).intValue());
            //result.append(StringUtils.center(row.get(i), getMaxSize(i) + 2));
            //System.out.println(getMaxSize(i));
            result.append("|");
        }
        result.append("\n");
        return result.toString();
    }

    public String toString()
    {
        StringBuilder result = new StringBuilder();
        result.append(formatRuleNew());
        result.append(formatRow(headers));
        result.append(formatRule());
        for (List<String> row : data) {
            result.append(formatRowNew(row));
        }
        result.append(formatRule());
        return result.toString();
    }
}
