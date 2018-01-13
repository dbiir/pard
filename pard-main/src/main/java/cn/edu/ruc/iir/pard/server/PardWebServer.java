package cn.edu.ruc.iir.pard.server;

import cn.edu.ruc.iir.pard.web.PardServlet;
import cn.edu.ruc.iir.pard.web.StaticServlet;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.webapp.WebAppContext;

import javax.servlet.ServletException;

public class PardWebServer
        implements Runnable
{
    private final int port;
    private Server jettyServer;

    public PardWebServer(int port)
    {
        this.port = port;
    }

    @Override
    public void run()
    {
        jettyServer = new Server(this.port);
        WebAppContext context = new WebAppContext();
        context.setContextPath("/");
        //context.setDescriptor("E:/share/test/struts2-blank/WEB-INF/web.xml");
        //context.setResourceBase("E:/share/test/struts2-blank");
        //ResourceHandler r = new ResourceHandler();
        ServletHolder staticHolder = new ServletHolder(new StaticServlet());
        context.addServlet(staticHolder, "*.html");
        context.addServlet(staticHolder, "*.js");
        context.addServlet(staticHolder, "*.css");
        context.addServlet(staticHolder, "*.jpg");
        context.setResourceBase(".");
        PardServlet s = new PardServlet();
        try {
            s.test();
        }
        catch (ServletException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        ServletHolder pardHolder = new ServletHolder(s);
        context.addServlet(pardHolder, "*.pard");
        //context.setParentLoaderPriority(true);
        jettyServer.setHandler(context);
        try {
            jettyServer.start();
            jettyServer.join();
        }
        catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void stop()
    {
        if (jettyServer != null) {
            try {
                jettyServer.stop();
            }
            catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }
    public static void main(String[] args)
    {
        new PardWebServer(10080).run();
    }
}
