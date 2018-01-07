package cn.edu.ruc.iir.pard.exchange;

import io.netty.handler.codec.marshalling.DefaultMarshallerProvider;
import io.netty.handler.codec.marshalling.DefaultUnmarshallerProvider;
import io.netty.handler.codec.marshalling.MarshallerProvider;
import io.netty.handler.codec.marshalling.MarshallingDecoder;
import io.netty.handler.codec.marshalling.MarshallingEncoder;
import io.netty.handler.codec.marshalling.UnmarshallerProvider;
import org.jboss.marshalling.MarshallerFactory;
import org.jboss.marshalling.Marshalling;
import org.jboss.marshalling.MarshallingConfiguration;

/**
 * pard
 *
 * @author guodong
 */
public class MarshallingCodecFactory
{
    private MarshallingCodecFactory()
    {}

    public static MarshallingDecoder buildMarshallingDecoder()
    {
        MarshallerFactory marshallerFactory = Marshalling.getProvidedMarshallerFactory("seria");
        final MarshallingConfiguration configuration = new MarshallingConfiguration();
        configuration.setVersion(5);
        UnmarshallerProvider unmarshallerProvider = new DefaultUnmarshallerProvider(marshallerFactory, configuration);
        return new MarshallingDecoder(unmarshallerProvider, 11 * 1024 * 1024);
    }

    public static MarshallingEncoder buildMarshallingEncoder()
    {
        MarshallerFactory marshallerFactory = Marshalling.getProvidedMarshallerFactory("seria");
        final MarshallingConfiguration configuration = new MarshallingConfiguration();
        configuration.setVersion(5);
        MarshallerProvider marshallerProvider = new DefaultMarshallerProvider(marshallerFactory, configuration);
        return new MarshallingEncoder(marshallerProvider);
    }
}
