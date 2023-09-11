package com.zyd.common;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * @program: base
 * @description:
 * @author: zhongyuandong
 * @create: 2023-09-08 17:43:19
 * @Version 1.0
 **/
public class Test {

    public static void main(String[] args) {
        String host = "qa-cdh-002";
        int port = 3306;

        telnet(host, port, 3000);
        try {
            // 创建Socket连接
            Socket socket = new Socket(host, port);

            // 获取输入流和输出流
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            PrintWriter out = new PrintWriter(socket.getOutputStream());

            // 读取服务器的响应
            String response = in.readLine();
            System.out.println("服务器响应: " + response);

            // 发送命令
            out.println("GET / HTTP/1.1");
            out.println("Host: " + host);
            out.println(); // 发送空行以结束命令
            out.flush(); // 刷新输出流

            // 读取命令执行结果
            String line;
            while ((line = in.readLine()) != null) {
                System.out.println("命令执行结果: " + line);
            }

            // 关闭连接
            in.close();
            out.close();
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 测试telnet 机器端口的连通性
     * @param hostname
     * @param port
     * @param timeout
     * @return
     */
    public static boolean telnet(String hostname, int port, int timeout){
        Socket socket = new Socket();
        boolean isConnected = false;
        try {
            socket.connect(new InetSocketAddress(hostname, port), timeout); // 建立连接
            isConnected = socket.isConnected(); // 通过现有方法查看连通状态
//            System.out.println(isConnected);    // true为连通
        } catch (IOException e) {
            System.out.println("false");        // 当连不通时，直接抛异常，异常捕获即可
        }finally{
            try {
                socket.close();   // 关闭连接
            } catch (IOException e) {
                System.out.println("false");
            }
        }
        System.out.println("telnet "+ hostname + " " + port + "\n==>isConnected: " + isConnected);
        return isConnected;
    }
}
