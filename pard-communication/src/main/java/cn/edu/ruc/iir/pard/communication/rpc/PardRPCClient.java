package cn.edu.ruc.iir.pard.communication.rpc;

import cn.edu.ruc.iir.pard.catalog.Column;
import cn.edu.ruc.iir.pard.communication.proto.PardGrpc;
import cn.edu.ruc.iir.pard.communication.proto.PardProto;
import cn.edu.ruc.iir.pard.executor.connector.CreateSchemaTask;
import cn.edu.ruc.iir.pard.executor.connector.CreateTableTask;
import cn.edu.ruc.iir.pard.executor.connector.DropSchemaTask;
import cn.edu.ruc.iir.pard.executor.connector.DropTableTask;
import cn.edu.ruc.iir.pard.executor.connector.InsertIntoTask;
import cn.edu.ruc.iir.pard.executor.connector.LoadTask;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import java.util.logging.Logger;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * pard
 *
 * @author guodong
 */
public class PardRPCClient
{
    private final ManagedChannel channel;
    private final PardGrpc.PardBlockingStub blockingStub;
    private final Logger logger = Logger.getLogger(PardRPCClient.class.getName());

    public PardRPCClient(String host, int port)
    {
        this(ManagedChannelBuilder.forAddress(host, port)
        .usePlaintext(true)
        .build());
    }

    private PardRPCClient(ManagedChannel channel)
    {
        this.channel = channel;
        this.blockingStub = PardGrpc.newBlockingStub(channel);
    }

    public int sendHeartBeat(int beatType, int nodeId, String message)
    {
        PardProto.HeartBeatMsg receiving;
        PardProto.HeartBeatMsg heartBeatMsg = PardProto.HeartBeatMsg.newBuilder()
                .setBeatType(beatType)
                .setNodeId(nodeId)
                .setMessage(message)
                .build();
        try {
            receiving = blockingStub.heartbeat(heartBeatMsg);
        }
        catch (StatusRuntimeException e) {
            e.printStackTrace();
            receiving = PardProto.HeartBeatMsg.newBuilder().build();
        }

        logger.info(
                toStringHelper(this)
                        .add("type", receiving.getBeatType())
                        .add("node", receiving.getNodeId())
                        .add("msg", receiving.getMessage())
                        .toString());

        return receiving.getBeatType();
    }

    public int createSchema(CreateSchemaTask task)
    {
        PardProto.ResponseStatus receiving;
        PardProto.SchemaMsg schemaMsg = PardProto.SchemaMsg.newBuilder()
                .setName(task.getSchemaName())
                .setIsNotExists(task.isNotExists())
                .build();
        try {
            receiving = blockingStub.createSchema(schemaMsg);
        }
        catch (StatusRuntimeException e) {
            e.printStackTrace();
            receiving = PardProto.ResponseStatus.newBuilder()
                    .setStatus(-1)
                    .build();
        }

        logger.info("RPC call: " + task);

        return receiving.getStatus();
    }

    public int dropSchema(DropSchemaTask task)
    {
        PardProto.ResponseStatus receiving;
        PardProto.SchemaMsg schemaMsg = PardProto.SchemaMsg.newBuilder()
                .setName(task.getSchema())
                .build();

        try {
            receiving = blockingStub.dropSchema(schemaMsg);
        }
        catch (StatusRuntimeException e) {
            e.printStackTrace();
            receiving = PardProto.ResponseStatus.newBuilder()
                    .setStatus(-1)
                    .build();
        }

        logger.info("RPC call: " + task);

        return receiving.getStatus();
    }

    public int createTable(CreateTableTask task)
    {
        PardProto.ResponseStatus receiving;
        PardProto.TableMsg.Builder tableMsgBuilder = PardProto.TableMsg.newBuilder()
                .setSchemaName(task.getSchemaName())
                .setName(task.getTableName());

        for (Column column : task.getColumnDefinitions()) {
            PardProto.ColumnMsg columnMsg = PardProto.ColumnMsg.newBuilder()
                    .setColumnName(column.getColumnName())
                    .setColumnType(column.getDataType())
                    .setColumnLength(column.getLen())
                    .setIsPrimary(column.getKey() == 1)
                    .build();
            tableMsgBuilder.addColumns(columnMsg);
        }

        try {
            receiving = blockingStub.createTable(tableMsgBuilder.build());
        }
        catch (StatusRuntimeException e) {
            e.printStackTrace();
            receiving = PardProto.ResponseStatus.newBuilder()
                    .setStatus(-1)
                    .build();
        }

        logger.info("RPC call: " + task);

        return receiving.getStatus();
    }

    public int dropTable(DropTableTask task)
    {
        PardProto.ResponseStatus receiving;
        PardProto.TableMsg tableMsg = PardProto.TableMsg.newBuilder()
                .setSchemaName(task.getSchemaName())
                .setName(task.getTableName())
                .build();

        try {
            receiving = blockingStub.dropTable(tableMsg);
        }
        catch (StatusRuntimeException e) {
            e.printStackTrace();
            receiving = PardProto.ResponseStatus.newBuilder()
                    .setStatus(-1)
                    .build();
        }

        logger.info("RPC call: " + task);

        return receiving.getStatus();
    }

    public int insertInto(InsertIntoTask task)
    {
        PardProto.ResponseStatus receiving;
        PardProto.InsertMsg.Builder insertMsgBuilder = PardProto.InsertMsg.newBuilder()
                .setSchemaName(task.getSchemaName())
                .setTableName(task.getTableName());

        for (Column column : task.getColumns()) {
            PardProto.ColumnMsg columnMsg = PardProto.ColumnMsg.newBuilder()
                    .setColumnName(column.getColumnName())
                    .setColumnType(column.getDataType())
                    .setColumnLength(column.getLen())
                    .setIsPrimary(column.getKey() == 1)
                    .build();
            insertMsgBuilder.addColumns(columnMsg);
        }

        for (String[] v : task.getValues()) {
            PardProto.RowMsg.Builder rowMsgBuilder = PardProto.RowMsg.newBuilder();
            for (String c : v) {
                rowMsgBuilder.addColumnValues(c);
            }
            insertMsgBuilder.addRows(rowMsgBuilder.build());
        }

        try {
            receiving = blockingStub.insertInto(insertMsgBuilder.build());
        }
        catch (StatusRuntimeException e) {
            e.printStackTrace();
            receiving = PardProto.ResponseStatus.newBuilder()
                    .setStatus(-1)
                    .build();
        }

        logger.info("RPC call: " + task);

        return receiving.getStatus();
    }

    public int load(LoadTask task)
    {
        PardProto.ResponseStatus receiving;
        PardProto.LoadMsg request = PardProto.LoadMsg.newBuilder()
                .setSchemaName(task.getSchema())
                .setTableName(task.getTable())
                .addAllPath(task.getPaths())
                .build();
        try {
            receiving = blockingStub.load(request);
        }
        catch (StatusRuntimeException e) {
            e.printStackTrace();
            receiving = PardProto.ResponseStatus.newBuilder()
                    .setStatus(-1)
                    .build();
        }
        logger.info("RPC call: " + task);

        return receiving.getStatus();
    }

    public void shutdown()
    {
        this.channel.shutdown();
    }

    public void shutdownNow()
    {
        this.channel.shutdownNow();
    }
}
