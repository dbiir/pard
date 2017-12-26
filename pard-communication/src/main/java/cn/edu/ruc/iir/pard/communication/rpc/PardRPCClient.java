package cn.edu.ruc.iir.pard.communication.rpc;

import cn.edu.ruc.iir.pard.communication.proto.PardGrpc;
import cn.edu.ruc.iir.pard.communication.proto.PardProto;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

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
            receiving = PardProto.HeartBeatMsg.newBuilder().build();
        }

        System.out.println(
                toStringHelper(this)
                        .add("type", receiving.getBeatType())
                        .add("node", receiving.getNodeId())
                        .add("msg", receiving.getMessage()));

        return receiving.getBeatType();
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
