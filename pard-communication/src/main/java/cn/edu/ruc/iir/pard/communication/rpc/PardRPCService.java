package cn.edu.ruc.iir.pard.communication.rpc;

import cn.edu.ruc.iir.pard.catalog.Column;
import cn.edu.ruc.iir.pard.commons.utils.PardResultSet;
import cn.edu.ruc.iir.pard.communication.proto.PardGrpc;
import cn.edu.ruc.iir.pard.communication.proto.PardProto;
import cn.edu.ruc.iir.pard.executor.connector.Connector;
import cn.edu.ruc.iir.pard.executor.connector.CreateSchemaTask;
import cn.edu.ruc.iir.pard.executor.connector.CreateTableTask;
import cn.edu.ruc.iir.pard.executor.connector.InsertIntoTask;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.List;

/**
 * pard
 *
 * @author guodong
 */
public class PardRPCService
        extends PardGrpc.PardImplBase
{
    private Connector connector;
    public PardRPCService(Connector connector)
    {
        this.connector = connector;
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
        PardResultSet resultSet = connector.execute(task);
        responseStatusBuilder.setStatus(resultSet.getStatus().ordinal());
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
        PardResultSet resultSet = connector.execute(task);
        responseStatusBuilder.setStatus(resultSet.getStatus().ordinal());
        responseStatusStreamObserver.onNext(responseStatusBuilder.build());
        responseStatusStreamObserver.onCompleted();
    }

    @Override
    public void insertInto(PardProto.RowMsg rowMsg,
                           StreamObserver<PardProto.ResponseStatus> responseStatusStreamObserver)
    {
        PardProto.ResponseStatus.Builder responseStatusBuilder
                = PardProto.ResponseStatus.newBuilder();
        InsertIntoTask task = new InsertIntoTask();
        PardResultSet resultSet = connector.execute(task);
        responseStatusBuilder.setStatus(resultSet.getStatus().ordinal());
        responseStatusStreamObserver.onNext(responseStatusBuilder.build());
        responseStatusStreamObserver.onCompleted();
    }
}
