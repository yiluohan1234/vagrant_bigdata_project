package com.atguigu.v2;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class HBaseConnect {

    // 设置静态属性 hbase 连接
    public static Connection connection = null;

    static {
        // 使用配置文件的方法
        try {
            connection = ConnectionFactory.createConnection();
        } catch (IOException e) {
            System.out.println("连接获取失败");
            e.printStackTrace();
        }
    }

    /**
     * 连接关闭方法,用于进程关闭时调用
     * @throws IOException
     */
    public static void closeConnection() throws IOException {
        if (connection != null) {
            connection.close();
        }
    }

    public static void main(String[] args) throws IOException {
//        // 1. 创建配置对象
//        Configuration conf = new Configuration();
//
//        // 2. 添加配置参数
//        conf.set("hbase.zookeeper.quorum", "hdp101,hdp102,hdp103");
//
//        // 3. 创建 hbase 的连接
//        // 默认使用同步连接
//        Connection connection = ConnectionFactory.createConnection(conf);
//
//        // 可以使用异步连接
//        // 主要影响后续的 DML 操作
//        CompletableFuture<AsyncConnection> asyncConnection = ConnectionFactory.createAsyncConnection(conf);
//
//        // 4. 使用连接
//        System.out.println(connection);
//
//        // 5. 关闭连接
//        connection.close();
        System.out.println(connection);
        closeConnection();
    }


}
