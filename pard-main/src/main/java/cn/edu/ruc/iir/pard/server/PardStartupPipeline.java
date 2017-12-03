package cn.edu.ruc.iir.pard.server;

import java.util.ArrayList;
import java.util.List;

/**
 * pard
 *
 * @author guodong
 */
public class PardStartupPipeline
{
    private final List<PardStartupHook> hooks;

    PardStartupPipeline()
    {
        hooks = new ArrayList<PardStartupHook>();
    }

    void addStartupHook(PardStartupHook hook)
    {
        hooks.add(hook);
    }

    void startup()
    {
        for (PardStartupHook hook : hooks) {
            hook.startup();
        }
    }
}
