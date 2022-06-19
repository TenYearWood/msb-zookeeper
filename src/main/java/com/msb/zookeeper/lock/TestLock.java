package com.msb.zookeeper.lock;

import com.msb.zookeeper.config.ZKUtils;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Zookeeper实现分布式锁
 */
public class TestLock {
    ZooKeeper zk;

    @Before
    public void conn(){
        zk = ZKUtils.getZK();
    }

    @After
    public void close(){
        try {
            zk.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void lock(){
        for (int i = 0; i < 10; i++) {
            Thread t = new Thread(() -> {
                String threadName = Thread.currentThread().getName();
                WatchCallBack watchCallBack = new WatchCallBack();
                watchCallBack.setZk(zk);
                watchCallBack.setThreadName(threadName);
                //抢锁
                watchCallBack.tryLock();
                //干活
                System.out.println("gan huo ...");
                //释放锁
                watchCallBack.unLock();

            });
            t.start();
        }
    }



}
