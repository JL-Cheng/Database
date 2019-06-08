package database.client;

import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.*;

import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.BufferedReader;
import java.io.PrintStream;
import java.net.Socket;
import java.net.URL;
import javax.swing.*;

/**
 * 类型：类
 * 功能：客户端GUI
 */

public class ClientGUI extends JFrame implements ActionListener
{
	private static final long serialVersionUID = 1L;//控制序列化版本
	
    JButton send_button; //发送按钮
    JButton clear_button; //清除sql
    JButton import_button; //import外部文件
    JTextArea input_area; //sql输入/显示区域
    JTextArea output_area;  //查询结果
    ImageIcon ok_icon;
    ImageIcon no_icon;
    JRadioButtonMenuItem connect;
    JRadioButtonMenuItem disconnect;
    JRadioButtonMenuItem show;
    JRadioButtonMenuItem hide;
    JLabel state_label;
    JLabel timer_label;

    String server_address;
    int server_port;
    private Socket socket = null; //this.socket对象
    private PrintStream out_socket;//this.socket输出流
    private BufferedReader in_socket; //this.socket输入流
    boolean isConnected; //是否连接服务器
    boolean isShow; //是否显示计时器

    /**
     * 构造函数
     * 初始化组件、添加事件监听
     */
    public  ClientGUI()
    {
        isConnected = false;
        isShow = true;
        //菜单
        JMenuBar menuBar = new JMenuBar();
        this.setJMenuBar(menuBar);
        //数据库
        JMenu server = new JMenu("数据库");
        menuBar.add(server);
        connect = new JRadioButtonMenuItem("连接数据库");
        disconnect = new JRadioButtonMenuItem("断开数据库");
        server.add(connect);
        server.addSeparator();
        server.add(disconnect);
        ButtonGroup btn_group1 = new ButtonGroup();
        btn_group1.add(connect);
        btn_group1.add(disconnect);
        disconnect.setSelected(true);
        connect.addActionListener(this);
        disconnect.addActionListener(this);

        //计时器
        JMenu timer = new JMenu("计时");
        menuBar.add(timer);
        show = new JRadioButtonMenuItem("显示计时器");
        hide = new JRadioButtonMenuItem("隐藏计时器");
        timer.add(show);
        timer.add(hide);
        ButtonGroup btn_group2 = new ButtonGroup();
        btn_group2.add(show);
        btn_group2.add(hide);
        show.setSelected(true);
        show.addActionListener(this);
        hide.addActionListener(this);
        timer_label = new JLabel("耗时:");
        JPanel timer_panel = new JPanel();
        timer_panel.setLayout(new FlowLayout(FlowLayout.LEFT));
        timer_panel.add(timer_label);

        //图标
        URL ok = getClass().getResource("ok.png");
        URL no = getClass().getResource("no.png");
        ok_icon = new ImageIcon(ok);
        no_icon = new ImageIcon(no);

        //按钮
        state_label = new JLabel();
        state_label.setIcon(no_icon);
        state_label.setText("等待连接服务器...");
        send_button = new JButton("发送");
        clear_button = new JButton("清除");
        import_button = new JButton("导入");
        send_button.addActionListener(this);//添加事件监听
        clear_button.addActionListener(this);
        import_button.addActionListener(this);
        send_button.setEnabled(false); //关闭send按钮，连接服务器后激活
        JPanel button_panel = new JPanel();
        button_panel.setLayout(new FlowLayout(FlowLayout.LEFT));
        button_panel.add(send_button);
        button_panel.add(clear_button);
        button_panel.add(import_button);
        button_panel.add(state_label);
        button_panel.setBorder(BorderFactory.createLoweredBevelBorder());

        //输入
        input_area = new JTextArea(10,40);
        JLabel input_label = new JLabel("输入sql");
        JPanel input_panel = new JPanel();
        input_panel.setLayout(new FlowLayout(FlowLayout.LEFT));
        input_panel.add(input_label);
        JScrollPane input_spanel = new JScrollPane(input_area);

        //输出
        output_area = new JTextArea(10,40);
        //output_area.setEnabled(false);
        JLabel output_label = new JLabel("查询结果");
        JPanel output_panel = new JPanel();
        output_panel.setLayout(new FlowLayout(FlowLayout.LEFT));
        output_panel.add(output_label);
        JScrollPane output_spanel = new JScrollPane(output_area);

        setTitle("SimpleSQL");
        JPanel panel = new JPanel();
        panel.setBorder(BorderFactory.createEmptyBorder(10, 10, 10, 10));
        panel.setLayout(new BoxLayout(panel, BoxLayout.Y_AXIS));
        panel.add(button_panel);
        panel.add(input_panel);
        panel.add(input_spanel);
        panel.add(output_panel);
        panel.add(output_spanel);
        panel.add(timer_panel);

        this.setContentPane(panel);
        this.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        this.setLocationRelativeTo(null);
    }

    /**
     * 事件处理函数
     * @param e 事件，包括点击发送、清除、导入按钮；连接、端口数据库菜单
     */
    public void actionPerformed(ActionEvent e)
    {
        Object source = e.getSource();
        if(source == send_button)
        {
            System.out.println("发送sql");
            sendRequest();
        }
        else if(source == clear_button)
        {
            System.out.println("清除sql");
            input_area.setText("");
            output_area.setText("");
        }
        else if(source == import_button)
        {
            importSql();
        }
        else if(isConnected == false && source == connect)
        {
           newConnectGUI();
        }
        else if(isConnected == true && source == disconnect)
        {
            disconnect();
        }
        else if(isShow == true && source == hide)
        {
            isShow = false;
            timer_label.setVisible(false);

        }
        else if(isShow == false && source == show)
        {
            isShow = true;
            timer_label.setVisible(true);
        }

    }

    /**
     * 连接服务器
     * @return 连接是否成功
     */
    private boolean connect ()
    {
        try
        {
            System.out.println("连接到主机：" + server_address + " ，端口号：" + server_port);
            this.socket = new Socket(server_address, server_port);
            this.out_socket = new PrintStream(this.socket.getOutputStream());
            this.in_socket = new BufferedReader(new InputStreamReader(this.socket.getInputStream()));


            //更新图标和连接状态
            state_label.setIcon(ok_icon);
            state_label.setText("已连接数据库!");
            isConnected = true;

            //激活发送键
            send_button.setEnabled(true);

            return true;
        }
        catch(IOException e)
        {
            e.printStackTrace();
            JOptionPane.showMessageDialog(null,"服务器连接错误️️","错误",JOptionPane.ERROR_MESSAGE);
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
                output_area.setText("");
                receiveResponse();
                this.socket.close();
                this.out_socket.close();
                this.in_socket.close();

                //更新图标和状态
                state_label.setIcon(no_icon);
                state_label.setText("等待连接服务器...");
                isConnected = false;

                //将发送键置为不可响应
                send_button.setEnabled(false);
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }
        }
    }

    /***
     * 接收服务器返回结果，并显示在界面上
     */
    private void receiveResponse()
    {
        try
        {
            String response = this.in_socket.readLine();
            while(!response.isEmpty())
            {
                output_area.append(response);
                output_area.append("\n");
                response = this.in_socket.readLine();
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    /**
     * 将输入区域的所有sql命令依次发送到服务器,并接收响应
     *
     */
    private void sendRequest()
    {
        //清空旧响应
        output_area.setText("");

        String input = input_area.getText();
        String[] sql_list = input.split("\\s*;\\s*");

        //开始计时
        long start_time = System.currentTimeMillis();

        for (String sql: sql_list)
        {
            if(sql.equals("exit"))
            {
                disconnect.setSelected(true);
                disconnect();
                break;
            }
            sql = sql.replaceAll("\n","");
            this.out_socket.println(sql);
            receiveResponse();

        }

        //结束计时
        long end_time = System.currentTimeMillis();
        long total_time = end_time - start_time;
        System.out.println("import执行时间为:"+total_time+"ms");
        timer_label.setText("耗时:"+total_time+"ms");
    }

    /**
     * 导入外部sql文件，将sql语句显示在输入框中
     * ！！！！此时并没有发送sql语句
     * ！！！！需要再次点击发送按钮，发送sql
     */
    private void importSql()
    {
        System.out.println("导入外部sql文件");
        JFileChooser fc = new JFileChooser();
        fc.setFileSelectionMode(JFileChooser.FILES_ONLY);
        fc.showDialog(new JLabel(), "选择");
        File file = fc.getSelectedFile();

        if(file == null)
            return;

        try
        {
            BufferedReader fin = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
            String sql_all = "";
            while(true)
            {
            	char[] sql_buf = new char[4096];
                int num = fin.read(sql_buf);
                if(num == -1)
                {
                	break;
                }
                sql_all += String.valueOf(sql_buf,0,num);          
            }
            input_area.setText(sql_all);
            fin.close();
        }
        catch (IOException err)
        {
            err.printStackTrace();
        }
    }

    /**
     * 创建新窗口，获取连接服务器信息
     */
    private void newConnectGUI()
    {
        //禁用原窗口
        this.setEnabled(false);
        this.setModalExclusionType(Dialog.ModalExclusionType.NO_EXCLUDE);

        //组件
        JFrame connectGUI = new JFrame("连接数据库");
        JLabel address_label = new JLabel("服务器地址");
        JTextField address_input = new JTextField(20);
        JLabel port_label = new JLabel("服务器端口");
        JTextField port_input = new JTextField(20);
        JButton connect = new JButton("连接");
        connect.addActionListener(new ActionListener()
        {
            @Override
            public void actionPerformed(ActionEvent e)
            {
                String address = address_input.getText();
                String port = port_input.getText();
                if(address.isEmpty()||port.isEmpty())
                {
                    JOptionPane.showMessageDialog(null,"服务器地址不能为空️️","错误",JOptionPane.ERROR_MESSAGE);
                }
                else
                {
                    server_address = address;
                    server_port= Integer.parseInt(port);
                    connect();
                    connectGUI.dispose();

                    //恢复原窗口响应
                    ClientGUI.this.setEnabled(true);
                }

            }
        });

        //布局
        JPanel panel = new JPanel();
        panel.setLayout(new BoxLayout(panel,BoxLayout.Y_AXIS));
        panel.add(Box.createVerticalStrut(10));
        panel.add(address_label);
        panel.add(address_input);
        panel.add(port_label);
        panel.add(port_input);
        panel.add(Box.createVerticalStrut(10));
        panel.add(connect);
        panel.add(Box.createVerticalStrut(10));
        connectGUI.setContentPane(panel);
        connectGUI.pack();
        connectGUI.setVisible(true);
        connectGUI.setLocationRelativeTo(null);
        connectGUI.addWindowListener(new WindowAdapter()
        {
            @Override
            public void windowClosing(WindowEvent e)
            {
                ClientGUI.this.setEnabled(true);
                disconnect.setSelected(true);
            }
        });
    }

    public static void main(String[] args)
    {
        try
        {
            String lookAndFeel = UIManager.getSystemLookAndFeelClassName();//当前系统的主题
            UIManager.setLookAndFeel(lookAndFeel);
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
        ClientGUI client = new ClientGUI();
        client.pack();
        client.setVisible(true);
    }
}
