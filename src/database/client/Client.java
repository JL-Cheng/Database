package database.client;

import java.net.*;
import java.io.*;

public class Client
{

    private String servername = null;//服务器地址
    private int port = -1;//端口号
    private Socket socket = null; //this.socket对象
    private PrintStream out_socket;//this.socket输出流
    private BufferedReader in_socket; //this.socket输入流
    private BufferedReader in_system = new BufferedReader(new InputStreamReader(System.in)); //标准输入
    private PrintStream out_system = System.out;//标准输出

    /**
     * 构造函数
     * @param servername 服务器地址
     * @param port 服务器端口
     */
    public Client(String servername, int port)
    {
        this.servername = servername;
        this.port = port;
    }

    /**
     * 连接服务器
     * @return 连接是否成功
     */
    private boolean connect ()
    {
        if(this.socket != null && this.socket.isConnected())
            return false;

        try
        {
            this.out_system.println("连接到主机：" + servername + " ，端口号：" + port);
            this.socket = new Socket(servername, port);
            //System.out.println("远程主机地址：" + this.socket.getRemotethis.socketAddress());
            this.out_socket = new PrintStream(this.socket.getOutputStream());
            this.in_socket = new BufferedReader(new InputStreamReader(this.socket.getInputStream()));
            return true;
        }
        catch(IOException e)
        {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * 断开服务器连接
     */
    private void disconnect ()
    {
        if(this.socket.isConnected())
        {
            try
            {
                this.out_socket.println("exit");
                receiveResponse();
                this.socket.close();
                this.out_socket.close();
                this.in_socket.close();
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }
        }
    }

    /**
     * 发送sql至服务器
     * @param request 待发送sql
     */
    private void sendRequest(String request)
    {
        this.out_socket.println(request);
    }

    /***
     * 接收服务器返回结果，并打印至标准输出流
     */
    private void receiveResponse()
    {
        try
        {
            String response = this.in_socket.readLine();
            while(!response.isEmpty())
            {
            	this.out_system.println(response);
            	response = this.in_socket.readLine();
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    /**
     * import命令
     * import文件最大为4k
     * @param request 输入命令，eg:"import XXX.txt"
     */
    private void importSql(String request)
    {
        String filename = request.split("\\s+")[1];
        try
        {
            BufferedReader fin = new BufferedReader(new InputStreamReader(new FileInputStream(filename)));
            //开始计时
            long start_time = System.currentTimeMillis();

            char[] sql_buf = new char[4096];
            int num = fin.read(sql_buf);
            String sql_all = String.valueOf(sql_buf,0,num);
            String[] sql_list = sql_all.split("\\s*;\\s*");

            for (String sql: sql_list)
            {
                sql = sql.replaceAll("\n","");//去掉换行符
                sendRequest(sql);
                receiveResponse();
            }
            //结束计时
            long end_time = System.currentTimeMillis();
            long total_time = end_time - start_time;
            this.out_system.println("import执行时间为:"+total_time+"ms");
            fin.close();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    /**
     * 从控制台读入SQL语句，以分号结束
     * @return 读入的SQL语句
     */
    private String readSql()
    {
        String result = "";
        try
        {
        	result = this.in_system.readLine();
        	while(!result.contains(";"))
        	{
        		result += this.in_system.readLine();
        	}
        	String[] sql_list = result.split("\\s*;\\s*");
        	result = sql_list[0];
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        return result;
    }
    
    /**
     * 和服务器收发数据
     */
    private void run()
    {
        try
        {
        	this.out_system.print(">>");
            String sql = this.readSql();
            while(!sql.equals("exit"))
            {
                if (sql.startsWith("import"))
                {
                    importSql(sql);
                }
                else
                {
                    //开始计时
                    long start_time = System.currentTimeMillis();
                    sendRequest(sql);
                    receiveResponse();
                    //结束计时
                    long end_time = System.currentTimeMillis();
                    long total_time = end_time - start_time;
                    this.out_system.println("import执行时间为:"+total_time+"ms");

                }
                this.out_system.print(">>");
                sql = this.readSql();
            }
            disconnect();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }


    public static void main(String [] args)
    {
        String servername = args[0];
        int port = Integer.parseInt(args[1]);
        Client client = new Client(servername, port);
        if (client.connect())
            client.run();
    }
}
