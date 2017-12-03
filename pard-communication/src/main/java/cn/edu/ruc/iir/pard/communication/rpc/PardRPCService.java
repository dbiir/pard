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
                                              StreamObserver<PardProto.ResponseStatus> responseStreamObserve)
    {
        responseStreamObserve.onNext(PardProto.ResponseStatus.newBuilder().build());
        responseStreamObserve.onCompleted();
    }
}
