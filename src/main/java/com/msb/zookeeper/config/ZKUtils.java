package com.msb.zookeeper.config;

import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * @description com.msb.zookeeper.config
 * @author: chengyu
 * @date: 2022-06-19 8:40
 */
public class ZKUtils {

    private static ZooKeeper zk;
    /*private static String address = "127.0.0.1:2181/testConf";*/
    private static String address = "127.0.0.1:2181/testLock";
    private static DefaultWatch watch = new DefaultWatch();
    private static CountDownLatch init = new CountDownLatch(1);

    /**
     * zookeeper是异步初始化的，异步直接返回zk，为防止别人拿到初始化后但是还没成功连接的zk，因此使用countDownLatch
     * @return
     */
    public static ZooKeeper getZK() {
        try {
            zk = new ZooKeeper(address, 1000, watch);
            watch.setCd(init);
            init.await();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return zk;
    }

}
