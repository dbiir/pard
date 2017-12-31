package cn.edu.ruc.iir.pard.commons.config;

import java.io.FileInputStream;
import java.io.IOException;

/**
 * pard
 *
 * @author guodong
 */
public final class PardUserConfiguration
        extends PardConfiguration
{
    private static class PardUserConfigurationHolder
    {
        private static final PardUserConfiguration instance = new PardUserConfiguration();
    }

    private PardUserConfiguration()
    {
    }

    private boolean validate()
    {
        // todo validate all configuration values
        return true;
    }

    public static final PardUserConfiguration INSTANCE()
    {
        return PardUserConfigurationHolder.instance;
    }

    public void init(String configurationPath)
    {
        // todo read config and validate
        try {
            configProps.load(new FileInputStream(configurationPath));
        }
        catch (IOException e) {
            System.out.println("Config file read faield");
        }
    }

    public String getNodeName()
    {
        return getProperty("pard.name");
    }

    public int getRPCPort()
    {
        return Integer.parseInt(getProperty("pard.rpc.port"));
    }

    public int getSocketPort()
    {
        return Integer.parseInt(getProperty("pard.server.port"));
    }

    public int getExchangePort()
    {
        return Integer.parseInt(getProperty("pard.exchange.port"));
    }

    public String getConnectorHost()
    {
        return getProperty("pard.connector.host");
    }

    public String getConnectorUser()
    {
        return getProperty("pard.connector.user");
    }

    public String getConnectorPassword()
    {
        return getProperty("pard.connector.password");
    }

    public String getConnectorDriver()
    {
        return getProperty("pard.connector.driver");
    }

    /**
     * name:host:ip, name:host:ip, ...
     * */
    public String getEtcdServers()
    {
        return getProperty("pard.etcd.servers");
    }

    public String getExchangeServers()
    {
        return getProperty("pard.exchange.servers");
    }

    public String getRPCServers()
    {
        return getProperty("pard.rpc.servers");
    }
}
