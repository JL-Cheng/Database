package database.server;


import java.net.*;
import  java.io.*;
import database.server.DatabaseManager;
import database.operation.Parser;

public class Server
{
    private ServerSocket listening_socket;
    private int port = -1;//端口号
    private Socket data_socket = null; //Socket对象
    private PrintStream out_socket;//socket输出流
    private BufferedReader in_socket; //socket输入流

    private DatabaseManager manager;//数据库管理器
    private Parser parser;//SQL语句解析器

    /***
     * 构造函数
     * @param port 服务端监听端口号
     * @param m 数据库管理器
     */
    public Server(int port,DatabaseManager m)
    {
        this.port = port;
        this.manager = m;
        this.parser = new Parser(this.manager);
        try
        {
            this.listening_socket = new ServerSocket(port);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    /**
     * 接收客户端输入的sql语句
     * @return  客户端sql语句
     */
    public String receiveRequest()
    {
        try
        {
            String sql = this.in_socket.readLine();
            System.out.println(sql);
            return sql;
        }
        catch (IOException e)
        {
            e.printStackTrace();
            return "";
        }
    }

    /**
     * 查询结果返回给客户端
     * @param res 查询结果
     */
    public void sendResponse (String  res)
    {
        this.out_socket.println(res);
    }

    /**
     * 解析客户端输入的sql
     * @param sql 接受的sql语句
     * @return  查询结果
     *          当对表操作时，返回"sql succeed/fail",eg:"create table XXX succeed"
     *          更新元组时，返回"table XXX update"
     *          查询元组时，返回"schema\n tuple1\n tuple2\n ...."
     */
    public String parseSql(String sql)
    {
    	try
    	{
    		String result = parser.processStatement(sql);
    		return result;
    	}
    	catch(Exception e)
    	{
    		return e.getMessage();
    	}
    }

    /**
     * 监听客户端，当监听到"exit"时停止监听
     */
    public void listening()
    {
        String sql = receiveRequest();
        while(!sql.equals("exit"))
        {
            String res = parseSql(sql);
            sendResponse(res);
            sql = receiveRequest();
        }
        sendResponse("goodbye!\n");
    }

    /**
     * 循环监听远程连接
     */
    public void run()
    {
        try
        {
        	while(true)
            {
                System.out.println("等待远程连接，端口号为" + port + "...");
                this.data_socket = listening_socket.accept();
                System.out.println("远端主机："+data_socket.getRemoteSocketAddress()+" 已连接");
                this.out_socket = new PrintStream(data_socket.getOutputStream());
                this.in_socket = new BufferedReader(new InputStreamReader(data_socket.getInputStream()));

                listening();
                this.data_socket.close();
                this.manager.database.close();
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }

    }

    public static void main(String [] args)
    {
        int port = 8888;
        if(args.length != 0)
        {
            port = Integer.parseInt(args[0]);
        }
        DatabaseManager manager = new DatabaseManager();
        Server server = new Server(port,manager);
        server.run();
    }

}
