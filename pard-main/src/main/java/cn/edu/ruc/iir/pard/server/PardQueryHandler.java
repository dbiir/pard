package cn.edu.ruc.iir.pard.server;

import cn.edu.ruc.iir.pard.communication.rpc.PardRPCClient;
import cn.edu.ruc.iir.pard.planner.PardPlanner;
import cn.edu.ruc.iir.pard.planner.Plan;
import cn.edu.ruc.iir.pard.scheduler.Task;
import cn.edu.ruc.iir.pard.scheduler.TaskGenerator;
import cn.edu.ruc.iir.pard.sql.parser.SqlParser;
import cn.edu.ruc.iir.pard.sql.tree.Statement;
import cn.edu.ruc.iir.pard.utils.PardResultSet;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * pard
 *
 * @author guodong
 */
public class PardQueryHandler
        extends Thread
{
    private Socket socket;
    private Logger logger = Logger.getLogger("pard server");
    private ObjectOutputStream objectOutputStream;
    {
        try {
            objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    private Map<Integer, PardRPCClient> rpcClients;
    private SqlParser sqlParser = new SqlParser();
    private PardPlanner planner = new PardPlanner();
    private TaskGenerator taskGenerator = new TaskGenerator();

    public PardQueryHandler(Socket socket)
    {
        this.socket = socket;
        this.rpcClients = new HashMap<>();
        // todo fill map with nodes
    }

    @Override
    public void run()
    {
        try (BufferedReader input = new BufferedReader(
                new InputStreamReader(socket.getInputStream()))) {
            while (true) {
                String line = input.readLine();
                if (line.equalsIgnoreCase("EXIT") ||
                        line.equalsIgnoreCase("QUIT")) {
                    logger.info("CLIENT QUIT");
                    break;
                }
                logger.info("QUERY: " + line);
                PardResultSet result = executeQuery(line);
                objectOutputStream.writeObject(result);
                objectOutputStream.flush();
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    private PardResultSet executeQuery(String sql)
    {
        // execute query
        logger.info("Executing query: " + sql);
        Statement statement = sqlParser.createStatement(sql);
        Plan plan = planner.plan(statement);
        List<Task> tasks = taskGenerator.generateTasks(plan);
        // todo distribute tasks and collect results

        return new PardResultSet(PardResultSet.ResultStatus.OK);
    }
}
