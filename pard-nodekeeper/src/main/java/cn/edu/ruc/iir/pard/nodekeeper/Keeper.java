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

    private Map<String, ServerInfo> etcdServers = null;
    private Map<String, ServerInfo> nettyServers = null;
    private Map<String, ServerInfo> rpcServers = null;

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
        etcdServers = new HashMap<String, ServerInfo>();
        nettyServers = new HashMap<String, ServerInfo>();
        rpcServers = new HashMap<String, ServerInfo>();
        load();
    }

    private void load()
    {
        // todo should load from catalog and sync
        String etcdServerStr = configuration.getEtcdServers();
        String nettyServerStr = configuration.getNettyServers();
        String rpcServerStr = configuration.getRPCServers();
        String[] etcdServerStrA = etcdServerStr.split(",");
        String[] nettyServerStrA = nettyServerStr.split(",");
        String[] rpcServerStrA = rpcServerStr.split(",");
        for (String server : etcdServerStrA) {
            ServerInfo serverInfo = new ServerInfo();
            server = server.trim();
            serverInfo.setName(server.split(":")[0]);
            serverInfo.setIp(server.split(":")[1]);
            serverInfo.setPort(Integer.parseInt(server.split(":")[2]));
            etcdServers.put(serverInfo.getName(), serverInfo);
        }
        for (String server : nettyServerStrA) {
            ServerInfo serverInfo = new ServerInfo();
            server = server.trim();
            serverInfo.setName(server.split(":")[0]);
            serverInfo.setIp(server.split(":")[1]);
            serverInfo.setPort(Integer.parseInt(server.split(":")[2]));
            nettyServers.put(serverInfo.getName(), serverInfo);
        }
        for (String server : rpcServerStrA) {
            ServerInfo serverInfo = new ServerInfo();
            server = server.trim();
            serverInfo.setName(server.split(":")[0]);
            serverInfo.setIp(server.split(":")[1]);
            serverInfo.setPort(Integer.parseInt(server.split(":")[2]));
            rpcServers.put(serverInfo.getName(), serverInfo);
        }
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
