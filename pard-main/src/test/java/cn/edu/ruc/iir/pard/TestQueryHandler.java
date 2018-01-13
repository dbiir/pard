package cn.edu.ruc.iir.pard;

import cn.edu.ruc.iir.pard.executor.connector.PardResultSet;
import cn.edu.ruc.iir.pard.scheduler.JobScheduler;
import cn.edu.ruc.iir.pard.scheduler.TaskScheduler;
import cn.edu.ruc.iir.pard.server.PardQueryHandler;
import org.testng.annotations.Test;

/**
 * pard
 *
 * @author guodong
 */
public class TestQueryHandler
{
    @Test
    public void executeQuery()
    {
        JobScheduler scheduler = JobScheduler.INSTANCE();
        TaskScheduler taskScheduler = TaskScheduler.INSTANCE();
        PardQueryHandler handler = new PardQueryHandler(null, scheduler, taskScheduler);
        String sql = "load \"/Users/Jelly/Downloads/eval_db/book.tsv\" into book.book";
        PardResultSet resultSet = handler.executeQuery(sql);
        System.out.println(resultSet);
    }
}
