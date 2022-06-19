package com.msb.zookeeper.lock;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @description com.msb.zookeeper.lock
 * @author: chengyu
 * @date: 2022-06-19 16:27
 */
public class WatchCallBack implements Watcher, AsyncCallback.StringCallback, AsyncCallback.Children2Callback, AsyncCallback.StatCallback {
    ZooKeeper zk;
    String threadName;
    CountDownLatch cc = new CountDownLatch(1);
    String pathName;

    /**
     * 如果第一个哥们，锁释放了，其实只有第二个收到了回调事件。
     * @param event
     */
    @Override
    public void process(WatchedEvent event) {
        switch (event.getType()) {
            case None:
                break;
            case NodeCreated:
                break;
            case NodeDeleted:
                zk.getChildren("/", false, this, "ctx");
                break;
            case NodeDataChanged:
                break;
            case NodeChildrenChanged:
                break;
        }
    }

    /**
     *
     * @param rc
     * @param path
     * @param ctx
     * @param name zk create完成后，创建成功后的节点名称
     */
    @Override
    public void processResult(int rc, String path, Object ctx, String name) {
        if (name != null) {
            System.out.println(threadName + "create node: " + name);
            pathName = name;
            /**
             * 所有线程不需要关注锁的目录的变化，所以不需要watch，为false
             * 但是回调是需要的
             */
            zk.getChildren("/", false, this, "ctx");
        }
    }

    //getChildren call back
    @Override
    public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {
        Collections.sort(children);
        int i = children.indexOf(pathName.substring(1));

        //如果为第一个
        if(i==0){
            System.out.println(threadName + " i am first...");
            cc.countDown();
        }else{
            /**
             * 如果不是第一个，看前一个创建的节点，设置watch
             * 前面一个节点一旦删除，锁释放了，触发NodeDeleted，回调getChildren call back，就重新排序，看下自己是不是第一个
             */
            zk.exists("/"+children.get(i-1), this, this, "ctx");
        }
    }

    @Override
    public void processResult(int rc, String path, Object ctx, Stat stat) {
        //todo
    }

    public void tryLock() {
        try {
            System.out.println(threadName + " create...");
            zk.create("/lock", threadName.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL, this, "ctx");

            cc.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void unLock() {
        try {
            zk.delete(pathName, -1);
            System.out.println(threadName + " over work...");
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    public ZooKeeper getZk() {
        return zk;
    }

    public void setZk(ZooKeeper zk) {
        this.zk = zk;
    }

    public String getThreadName() {
        return threadName;
    }

    public void setThreadName(String threadName) {
        this.threadName = threadName;
    }

    public String getPathName() {
        return pathName;
    }

    public void setPathName(String pathName) {
        this.pathName = pathName;
    }



}
