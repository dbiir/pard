package cn.edu.ruc.iir.pard.planner.dml;

import cn.edu.ruc.iir.pard.catalog.Column;
import cn.edu.ruc.iir.pard.catalog.Fragment;
import cn.edu.ruc.iir.pard.catalog.Schema;
import cn.edu.ruc.iir.pard.catalog.Table;
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
        }
        columns.sort(Comparator.comparingInt(Column::getId));

        Map<String, Fragment> fragments = table.getFragment();     // fragmentName -> fragment
        Map<String, BufferedWriter> tmpWriters = new HashMap<>();  // fragmentName -> writer
        Map<String, String> tmpPaths = new HashMap<>();            // fragmentName -> tmp file path
        String[] columnNames = new String[columns.size()];         // array of column names in order
        for (int i = 0; i < columns.size(); i++) {
            columnNames[i] = columns.get(i).getColumnName();
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

        // read into tmpfs
        try {
            BufferedReader reader = new BufferedReader(new FileReader(file));
            String line = null;
            while ((line = reader.readLine()) != null) {
                String[] values = line.split("\t");
                for (String f : fragments.keySet()) {
                    Fragment fragment = fragments.get(f);
                    if (ConditionComparator.matchString(fragment.getCondition(), columnNames, values)) {
                        tmpWriters.get(f).write(String.join("\t", values) + "\n");
                        break;
                    }
                }
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
