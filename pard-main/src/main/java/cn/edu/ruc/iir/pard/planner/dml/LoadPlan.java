package cn.edu.ruc.iir.pard.planner.dml;

import cn.edu.ruc.iir.pard.catalog.Column;
import cn.edu.ruc.iir.pard.catalog.Condition;
import cn.edu.ruc.iir.pard.catalog.Fragment;
import cn.edu.ruc.iir.pard.catalog.GddUtil;
import cn.edu.ruc.iir.pard.catalog.Schema;
import cn.edu.ruc.iir.pard.catalog.Table;
import cn.edu.ruc.iir.pard.commons.utils.DataType;
import cn.edu.ruc.iir.pard.etcd.dao.SchemaDao;
import cn.edu.ruc.iir.pard.etcd.dao.TableDao;
import cn.edu.ruc.iir.pard.planner.ConditionComparator;
import cn.edu.ruc.iir.pard.planner.ErrorMessage;
import cn.edu.ruc.iir.pard.planner.Plan;
import cn.edu.ruc.iir.pard.planner.ddl.UsePlan;
import cn.edu.ruc.iir.pard.sql.tree.Load;
import cn.edu.ruc.iir.pard.sql.tree.Statement;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * pard
 *
 * @author guodong
 */
public class LoadPlan
        extends Plan
{
    private String path;
    private String schemaName;
    private String tableName;
    private Map<String, List<String>> distributionHints;      // siteName -> tmpFilePath

    public LoadPlan(Statement stmt)
    {
        super(stmt);
    }

    @Override
    public ErrorMessage semanticAnalysis()
    {
        this.distributionHints = new HashMap<>();
        boolean checkSchema = false;
        Load load = (Load) getStatment();
        Schema schema = null;
        Table table = null;
        this.path = load.getPath().toString().replaceAll("\"|'", "");
        this.tableName = load.getTable().getSuffix();

        // check if file exists
        File file = new File(this.path);
        if (!file.exists() || file.isDirectory()) {
            return ErrorMessage.throwMessage(ErrorMessage.ErrCode.FileNotFound, this.path);
        }

        // check schema
        if (load.getTable().getPrefix().isPresent()) {
            this.schemaName = load.getTable().getPrefix().get().toString();
        }
        else {
            schema = UsePlan.getCurrentSchema();
            if (schema != null) {
                this.schemaName = schema.getName();
                checkSchema = true;
            }
        }
        if (this.schemaName == null) {
            return ErrorMessage.throwMessage(ErrorMessage.ErrCode.SchemaNotSpecified);
        }
        if (!checkSchema) {
            SchemaDao schemaDao = new SchemaDao();
            schema = schemaDao.loadByName(this.schemaName);
            if (schema == null) {
                return ErrorMessage.throwMessage(ErrorMessage.ErrCode.SchemaNotExsits, this.schemaName);
            }
        }

        // check table
        TableDao tableDao = new TableDao(schema);
        table = tableDao.loadByName(tableName);
        if (table == null) {
            return ErrorMessage.throwMessage(ErrorMessage.ErrCode.TableNotExists, tableName);
        }

        List<Column> columns = new ArrayList<>();
        for (String key : table.getColumns().keySet()) {
            columns.add(table.getColumns().get(key));
            //System.out.println("add column " + key);
        }
        columns.sort(Comparator.comparingInt(Column::getId));

        Map<String, Fragment> fragments = table.getFragment();     // fragmentName -> fragment
        Map<String, BufferedWriter> tmpWriters = new HashMap<>();  // fragmentName -> writer
        Map<String, String> tmpPaths = new HashMap<>();            // fragmentName -> tmp file path
        String[] columnNames = new String[columns.size()];         // array of column names in order
        Map<String, String> rowValues = new HashMap<>();           // column name -> column value
        for (int i = 0; i < columns.size(); i++) {
            String cName = columns.get(i).getColumnName();
            columnNames[i] = cName;
        }
        try {
            for (String fragmentName : fragments.keySet()) {
                String tmpPath = "/dev/shm/" + fragmentName + String.valueOf(System.currentTimeMillis());
                BufferedWriter writer = new BufferedWriter(new FileWriter(tmpPath));
                tmpWriters.put(fragmentName, writer);
                tmpPaths.put(fragmentName, tmpPath);
            }
        }
        catch (IOException e) {
            e.printStackTrace();
            return ErrorMessage.throwMessage(ErrorMessage.ErrCode.FileIOError);
        }
        Map<String, Integer> cnt = new HashMap<String, Integer>();
        for (String f : fragments.keySet()) {
            cnt.put(f, 0);
        }
        //check horizontal or vertical
        Map<String, List<String>> col2site = new HashMap<String, List<String>>();
        List<String> siteList = new ArrayList<String>();
        table.getFragment().values().forEach(x->siteList.add(x.getSiteName()));
        Map<String, List<Column>> colListMap = new HashMap<>();
        boolean isHorizontal = true;
        if (table.getFragment().values().iterator().next().getFragmentType() == GddUtil.fragementHORIZONTIAL) {
            isHorizontal = true;
            for (String site : siteList) {
                colListMap.put(site, new ArrayList<>());
            }
            for (Column col : table.getColumns().values()) {
                col2site.put(col.getColumnName(), siteList);
            }
        }
        else {
            isHorizontal = false;
            for (Column col : table.getColumns().values()) {
                col2site.put(col.getColumnName(), new ArrayList<>());
            }
            for (Fragment frag : table.getFragment().values()) {
                colListMap.put(frag.getSiteName(), new ArrayList<>());
                String siteName = frag.getSiteName();
                for (Condition cond : frag.getCondition()) {
                    //System.out.println("site " + siteName + " col " + cond.getColumnName() + table.getColumns().get(cond.getColumnName()).getId());
                    col2site.get(cond.getColumnName()).add(siteName);
                }
            }
            for (int i = 0; i < columnNames.length; i++) {
                //colLists.add(table.getColumns().get(key));
                Column col = table.getColumns().get(columnNames[i]);
                List<String> sites = col2site.get(col.getColumnName());
                for (String site : sites) {
                    colListMap.get(site).add(col);
                    //System.out.println("site " + site + " has col " + col.getColumnName());
                }
            }
        }
        // read into tmpfs
        try {
            BufferedReader reader = new BufferedReader(new FileReader(file));
            String line = null;
            Map<String, List<String>> distRow = null;
            while ((line = reader.readLine()) != null) {
                if (!isHorizontal) {
                    distRow = new HashMap<>();
                    for (String s : siteList) {
                        distRow.put(s, new ArrayList<>());
                    }
                }
                String[] values = line.split("\t");
                for (int i = 0; i < values.length; i++) {
                    if (columns.get(i).getDataType() == DataType.DataTypeInt.CHAR || columns.get(i).getDataType() == DataType.DataTypeInt.VARCHAR || columns.get(i).getDataType() == DataType.DataTypeInt.DATE) {
                        if (!values[i].startsWith("'") || !values[i].endsWith("'")) {
                            rowValues.put(columnNames[i], "'" + values[i] + "'");
                        }
                    }
                    else {
                        //System.out.println("i=" + i + "dataType " + columns.get(i).getDataType());
                        rowValues.put(columnNames[i], values[i]);
                    }
                    if (!isHorizontal) {
                        for (String site : col2site.get(columns.get(i).getColumnName())) {
                            distRow.get(site).add(values[i]);
                            //System.out.println("add " + values[i] + " i= " + i + " to site " + site + " columnNames[i]=" + columnNames[i]);
                        }
                    }
                }
                if (isHorizontal) {
                    for (String f : fragments.keySet()) {
                        Fragment fragment = fragments.get(f);
                        if (ConditionComparator.matchString(fragment.getCondition(), rowValues)) {
                            tmpWriters.get(f).write(String.join("\t", values) + "\n");
                            cnt.put(f, cnt.get(f) + 1);
                            break;
                        }
                    }
                }
                else {
                    for (String f : fragments.keySet()) {
                        Fragment fragment = fragments.get(f);
                        String siteName = fragment.getSiteName();
                        List<String> list = distRow.get(siteName);
                        String[] str = new String[list.size()];
                        for (int i = 0; i < list.size(); i++) {
                            str[i] = list.get(i);
                        }
                        tmpWriters.get(f).write(String.join("\t", str) + "\n");
                        //System.out.println(String.join("\t", str));
                        cnt.put(f, cnt.get(f) + 1);
                    }
                }
                break;
            }

            for (String f : fragments.keySet()) {
                System.out.println(f + "f " + cnt.get(f));
            }
            //close
            reader.close();
            for (BufferedWriter writer : tmpWriters.values()) {
                writer.close();
            }
        }
        catch (FileNotFoundException e) {
            e.printStackTrace();
            return ErrorMessage.throwMessage(ErrorMessage.ErrCode.FileNotFound, this.path);
        }
        catch (IOException e) {
            e.printStackTrace();
            return ErrorMessage.throwMessage(ErrorMessage.ErrCode.FileIOError);
        }

        for (String f : fragments.keySet()) {
            Fragment fragment = fragments.get(f);
            String siteName = fragment.getSiteName();
            List<String> fragmentPaths;
            if (distributionHints.containsKey(siteName)) {
                fragmentPaths = distributionHints.get(siteName);
            }
            else {
                fragmentPaths = new ArrayList<>();
                distributionHints.put(siteName, fragmentPaths);
            }
            String path = tmpPaths.get(f);
            fragmentPaths.add(path);
        }

        return ErrorMessage.getOKMessage();
    }
    public static String join(String d, String[] value)
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < value.length; i++) {
            sb.append(value[i]);
            if (i != value.length - 1) {
                sb.append(d);
            }
        }
        return sb.toString();
    }
    public String getPath()
    {
        return path;
    }

    public String getSchemaName()
    {
        return schemaName;
    }

    public String getTableName()
    {
        return tableName;
    }

    public Map<String, List<String>> getDistributionHints()
    {
        return distributionHints;
    }
}
