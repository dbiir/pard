package cn.edu.ruc.iir.pard.communication.rpc;

import cn.edu.ruc.iir.pard.communication.proto.PardGrpc;
import cn.edu.ruc.iir.pard.communication.proto.PardProto;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

/**
 * pard
 *
 * @author guodong
 */
public class PardRPCClient
{
    private final ManagedChannel channel;
    private final PardGrpc.PardBlockingStub blockingStub;

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
        PardProto.ResponseStatus status;
        PardProto.HeartBeatMsg heartBeatMsg = PardProto.HeartBeatMsg.newBuilder()
                .setBeatType(beatType)
                .setNodeId(nodeId)
                .setMessage(message)
                .build();
        try {
            status = blockingStub.heartbeat(heartBeatMsg);
        }
        catch (StatusRuntimeException e) {
            status = PardProto.ResponseStatus.newBuilder()
                    .setStatus(-1)
                    .build();
        }
        return status.getStatus();
    }
}
