package cn.edu.ruc.iir.pard.nodekeeper;

import java.util.HashMap;
import java.util.Map;

/**
 * 这个类的其他代码由crazy_bird 实现
 *
 * @author hagen
 * */
public class Keeper
{
    private Map<String, ServerInfo> etcdServers = null;
    private Map<String, ServerInfo> nettyServers = null;
    private Map<String, ServerInfo> rpcServers = null;

    public Keeper()
    {
        etcdServers = new HashMap<String, ServerInfo>();
        nettyServers = new HashMap<String, ServerInfo>();
        rpcServers = new HashMap<String, ServerInfo>();
    }
    public Map<String, ServerInfo> getEtcdServers()
    {
        return etcdServers;
    }

    public Map<String, ServerInfo> getNettyServers()
    {
        return nettyServers;
    }

    public Map<String, ServerInfo> getRpcServers()
    {
        return rpcServers;
    }
}
