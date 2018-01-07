package cn.edu.ruc.iir.pard.communication.rpc;

import cn.edu.ruc.iir.pard.catalog.Column;
import cn.edu.ruc.iir.pard.catalog.Schema;
import cn.edu.ruc.iir.pard.catalog.Table;
import cn.edu.ruc.iir.pard.communication.proto.PardGrpc;
import cn.edu.ruc.iir.pard.communication.proto.PardProto;
import cn.edu.ruc.iir.pard.etcd.dao.SchemaDao;
import cn.edu.ruc.iir.pard.executor.PardTaskExecutor;
import cn.edu.ruc.iir.pard.executor.connector.CreateSchemaTask;
import cn.edu.ruc.iir.pard.executor.connector.CreateTableTask;
import cn.edu.ruc.iir.pard.executor.connector.DropSchemaTask;
import cn.edu.ruc.iir.pard.executor.connector.DropTableTask;
import cn.edu.ruc.iir.pard.executor.connector.InsertIntoTask;
import cn.edu.ruc.iir.pard.executor.connector.PardResultSet;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * pard
 *
 * @author guodong
 */
public class PardRPCService
        extends PardGrpc.PardImplBase
{
    private final PardTaskExecutor executor;
    public PardRPCService(PardTaskExecutor executor)
    {
        this.executor = executor;
    }

    @Override
    public void heartbeat(PardProto.HeartBeatMsg heartBeatMsg,
                                              StreamObserver<PardProto.HeartBeatMsg> responseStreamObserve)
    {
        PardProto.HeartBeatMsg msg =
                PardProto.HeartBeatMsg.newBuilder()
                .setBeatType(2)
                .setNodeId(1)
                .setMessage("Hey there this is 1")
                .build();
        responseStreamObserve.onNext(msg);
        responseStreamObserve.onCompleted();
    }

    @Override
    public void createSchema(PardProto.SchemaMsg schemaMsg,
                             StreamObserver<PardProto.ResponseStatus> responseStatusStreamObserver)
    {
        PardProto.ResponseStatus.Builder responseStatusBuilder
                = PardProto.ResponseStatus.newBuilder();
        CreateSchemaTask task = new CreateSchemaTask(
                schemaMsg.getName(),
                schemaMsg.getIsNotExists());
        PardResultSet resultSet = executor.execute(task);
        if (resultSet.getStatus() == PardResultSet.ResultStatus.OK) {
            responseStatusBuilder.setStatus(1);
        }
        else {
            responseStatusBuilder.setStatus(-1);
        }
        responseStatusStreamObserver.onNext(responseStatusBuilder.build());
        responseStatusStreamObserver.onCompleted();
    }

    @Override
    public void dropSchema(PardProto.SchemaMsg schemaMsg,
                           StreamObserver<PardProto.ResponseStatus> responseStatusStreamObserver)
    {
        PardProto.ResponseStatus.Builder responseStatusBuilder
                = PardProto.ResponseStatus.newBuilder();
        DropSchemaTask task = new DropSchemaTask(
                schemaMsg.getName(),
                schemaMsg.getIsNotExists());
        PardResultSet resultSet = executor.execute(task);
        if (resultSet.getStatus() == PardResultSet.ResultStatus.OK) {
            responseStatusBuilder.setStatus(1);
        }
        else {
            responseStatusBuilder.setStatus(0);
        }
        responseStatusStreamObserver.onNext(responseStatusBuilder.build());
        responseStatusStreamObserver.onCompleted();
    }

    @Override
    public void createTable(PardProto.TableMsg tableMsg,
                            StreamObserver<PardProto.ResponseStatus> responseStatusStreamObserver)
    {
        PardProto.ResponseStatus.Builder responseStatusBuilder
                = PardProto.ResponseStatus.newBuilder();
        List<Column> columns = new ArrayList<>();
        for (PardProto.ColumnMsg columnMsg : tableMsg.getColumnsList()) {
            Column column = new Column();
            column.setColumnName(columnMsg.getColumnName());
            column.setDataType(columnMsg.getColumnType());
            column.setKey(columnMsg.getIsPrimary() ? 1 : 0);
            column.setLen(columnMsg.getColumnLength());
            columns.add(column);
        }
        CreateTableTask task = new CreateTableTask(
                tableMsg.getSchemaName(),
                tableMsg.getName(),
                tableMsg.getIsNotExists(),
                columns);
        PardResultSet resultSet = executor.execute(task);
        if (resultSet.getStatus() == PardResultSet.ResultStatus.OK) {
            responseStatusBuilder.setStatus(1);
        }
        else {
            responseStatusBuilder.setStatus(0);
        }
        responseStatusStreamObserver.onNext(responseStatusBuilder.build());
        responseStatusStreamObserver.onCompleted();
    }

    @Override
    public void dropTable(PardProto.TableMsg tableMsg,
                          StreamObserver<PardProto.ResponseStatus> responseStatusStreamObserver)
    {
        PardProto.ResponseStatus.Builder responseStatusBuilder
                = PardProto.ResponseStatus.newBuilder();
        DropTableTask task = new DropTableTask(tableMsg.getSchemaName(), tableMsg.getName());
        PardResultSet resultSet = executor.execute(task);
        if (resultSet.getStatus() == PardResultSet.ResultStatus.OK) {
            responseStatusBuilder.setStatus(1);
        }
        else {
            responseStatusBuilder.setStatus(0);
        }
        responseStatusStreamObserver.onNext(responseStatusBuilder.build());
        responseStatusStreamObserver.onCompleted();
    }

    @Override
    public void insertInto(PardProto.InsertMsg insertMsg,
                           StreamObserver<PardProto.ResponseStatus> responseStatusStreamObserver)
    {
        PardProto.ResponseStatus.Builder responseStatusBuilder
                = PardProto.ResponseStatus.newBuilder();
        List<Column> columns = new ArrayList<>();
        for (PardProto.ColumnMsg columnMsg : insertMsg.getColumnsList()) {
            Column column = new Column();
            column.setColumnName(columnMsg.getColumnName());
            column.setDataType(columnMsg.getColumnType());
            column.setLen(columnMsg.getColumnLength());
            columns.add(column);
        }
        int columnSize = columns.size();
        int rowSize = insertMsg.getRowsList().size();
        String[][] rows = new String[rowSize][columnSize];
        int index = 0;
        for (PardProto.RowMsg rowMsg : insertMsg.getRowsList()) {
            String[] row = new String[columnSize];
            for (int j = 0; j < columnSize; j++) {
                row[j] = rowMsg.getColumnValues(j);
            }
            rows[index] = row;
            index++;
        }
        InsertIntoTask task = new InsertIntoTask(
                insertMsg.getSchemaName(),
                insertMsg.getTableName(),
                columns, rows);
        PardResultSet resultSet = executor.execute(task);
        if (resultSet.getStatus() == PardResultSet.ResultStatus.OK) {
            responseStatusBuilder.setStatus(1);
        }
        else {
            responseStatusBuilder.setStatus(0);
        }
        responseStatusStreamObserver.onNext(responseStatusBuilder.build());
        responseStatusStreamObserver.onCompleted();
    }

    @Override
    public void showSchema(PardProto.NullMsg msg,
                           StreamObserver<PardProto.ResponseStatus> responseStatusStreamObserver)
    {
        PardProto.ResponseStatus.Builder responseStatusBuilder
                = PardProto.ResponseStatus.newBuilder();
        SchemaDao schemaDao = new SchemaDao();
        Set<String> schemas = schemaDao.listAll();
        schemas.forEach(responseStatusBuilder::addInfos);
        responseStatusBuilder.setStatus(1);
        responseStatusStreamObserver.onNext(responseStatusBuilder.build());
        responseStatusStreamObserver.onCompleted();
    }

    @Override
    public void showTable(PardProto.SchemaMsg msg,
                          StreamObserver<PardProto.ResponseStatus> responseStatusStreamObserver)
    {
        PardProto.ResponseStatus.Builder responseStatusBuilder
                = PardProto.ResponseStatus.newBuilder();
        SchemaDao schemaDao = new SchemaDao();
        Schema schema = schemaDao.loadByName(msg.getName());
        if (schema != null) {
            List<Table> tables = schema.getTableList();
            tables.forEach(t -> responseStatusBuilder.addInfos(t.getTablename()));
            responseStatusBuilder.setStatus(1);
        }
        else {
            responseStatusBuilder.setStatus(0);
        }
        responseStatusStreamObserver.onNext(responseStatusBuilder.build());
        responseStatusStreamObserver.onCompleted();
    }
}
