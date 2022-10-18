package com.atguigu.case1;


import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

public class zkTask {
    private static String connectString = "hdp101:2181,hdp102:2181,hdp103:2181";
    private static int sessionTimeout = 2000;

    public void init() throws IOException {
        ZooKeeper zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            public void process(WatchedEvent watchedEvent) {

            }
        });
    }
}
