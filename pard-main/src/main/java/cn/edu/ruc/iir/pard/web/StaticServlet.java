package cn.edu.ruc.iir.pard.web;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.io.InputStream;

public class StaticServlet
        extends HttpServlet
{
    /**
     *
     */
    private static final long serialVersionUID = 6154102774532361192L;
   // private URL url = null;
    @Override
    public void init() throws ServletException
    {
        super.init();
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException
    {
        String str = req.getRequestURI();
        if (str.startsWith("/")) {
            str = str.substring(1);
        }
        InputStream in = this.getClass().getClassLoader().getResourceAsStream(str);
        if (in != null) {
            int n = 0;
            byte[] b = new byte[4096];
            while ((n = in.read(b)) != -1) {
                resp.getOutputStream().write(b, 0, n);
            }
            in.close();
        }
        else {
            resp.getWriter().println(str + " not found!");
        }
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException
    {
        // TODO Auto-generated method stub
        super.doPost(req, resp);
    }
}
