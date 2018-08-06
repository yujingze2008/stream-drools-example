package com.gitee.code4fun;

import org.glassfish.grizzly.utils.BufferOutputStream;

import java.io.BufferedWriter;
import java.io.OutputStream;
import java.net.Socket;

/**
 * @author yujingze
 * @data 2018/8/6
 */
public class SocketMocker {

    public static void main(String[] args) throws Exception{

        Socket socket = new Socket("localhost",8888);

        OutputStream os = socket.getOutputStream();

        os.write("zhang111,900".getBytes());
        os.flush();
        //os.close();

        //socket.close();
    }

}
