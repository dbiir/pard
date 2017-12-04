package cn.edu.ruc.iir.pard.commons.config;

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
    {}

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
    }

    public int getServerPort()
    {
        return Integer.parseInt(getProperty("pard.server.port"));
    }
}
