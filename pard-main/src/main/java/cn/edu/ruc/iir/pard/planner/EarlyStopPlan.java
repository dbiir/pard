package cn.edu.ruc.iir.pard.planner;

public interface EarlyStopPlan
{
    public default boolean isAlreadyDone()
    {
        return false;
    }
}
