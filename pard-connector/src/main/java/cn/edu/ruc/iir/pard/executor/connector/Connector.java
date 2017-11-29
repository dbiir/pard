package cn.edu.ruc.iir.pard.executor.connector;

import cn.edu.ruc.iir.pard.sql.tree.Node;

/**
 * pard
 *
 * @author guodong
 */
public interface Connector
{
    void init();

    void execute(Node node);

    void close();
}
