package cn.edu.ruc.iir.pard.nodekeeper;

import cn.edu.ruc.iir.pard.commons.config.PardUserConfiguration;

import java.util.HashMap;
import java.util.Map;

/**
 * 这个类的其他代码由crazy_bird 实现
 *
 * @author hagen
 * */
public class Keeper
{
    private final PardUserConfiguration configuration = PardUserConfiguration.INSTANCE();

    private Map<String, ServerInfo> etcdServers;
    private Map<String, ServerInfo> exchangeServers;
    private Map<String, ServerInfo> rpcServers;

    private static final class KeeperHolder
    {
        private static final Keeper instance = new Keeper();
    }

    public static final Keeper INSTANCE()
    {
        return KeeperHolder.instance;
    }

    private Keeper()
    {
        etcdServers = new HashMap<>();
        exchangeServers = new HashMap<>();
        rpcServers = new HashMap<>();
        load();
    }

    private void load()
    {
        // todo should load from catalog and sync
    }

    public Map<String, ServerInfo> getEtcdServers()
    {
        return etcdServers;
    }

    public Map<String, ServerInfo> getExchangeServers()
    {
        return exchangeServers;
    }

    public Map<String, ServerInfo> getRpcServers()
    {
        return rpcServers;
    }
}
