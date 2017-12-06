package cn.edu.ruc.iir.pard.communication.rpc;

import cn.edu.ruc.iir.pard.communication.proto.PardGrpc;
import cn.edu.ruc.iir.pard.communication.proto.PardProto;
import io.grpc.stub.StreamObserver;

/**
 * pard
 *
 * @author guodong
 */
public class PardRPCService
        extends PardGrpc.PardImplBase
{
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
}
