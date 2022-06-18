package com.msb.zookeeper;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws IOException, InterruptedException, KeeperException {
        /**
         * zk是有session概念的，没有连接池的概念，因为每一个连接会得到独立的session
         * watch：观察，回调
         * watch的注册只发生在读类型调用，get，exists...
         * 第一类：new zk的时候，传入的watch，这个watch是session级别的，跟path、node没有关系
         */
        CountDownLatch cd = new CountDownLatch(1);
        ZooKeeper zk = new ZooKeeper("127.0.0.1:2181", 3000, new Watcher() {

            /**
             * watch的回调
             * 什么时候会被回调呢？zk连接成功状态之后会被回调，即SyncConnected
             */
            @Override
            public void process(WatchedEvent event) {
                Event.KeeperState state = event.getState();
                Event.EventType type = event.getType();
                String path = event.getPath();
                System.out.println("event: " + event.toString());

                //event的状态
                switch (state) {
                    case Unknown:
                        break;
                    case Disconnected:
                        break;
                    case NoSyncConnected:
                        break;
                    case SyncConnected:
                        System.out.println("connected");
                        cd.countDown();
                        break;
                    case AuthFailed:
                        break;
                    case ConnectedReadOnly:
                        break;
                    case SaslAuthenticated:
                        break;
                    case Expired:
                        break;
                }

                //event的类型
                switch (type) {
                    case None:
                        break;
                    case NodeCreated:
                        break;
                    case NodeDeleted:
                        break;
                    case NodeDataChanged:
                        break;
                    case NodeChildrenChanged:
                        break;
                }
            }
        });

        /**
         * 在这里阻塞住，直到SyncConnected事件发生
         */
        cd.await();

        /**
         * 打印出来的是ing ...，zk创建之后快速的异步返回zk，它还在connecting中，
         * 因此想要得到一个已经connected好的zk，使用countDownLatch
         * 真正要用zk之前，用latch在上面阻塞住，直到集群回调了事件之后，SyncConnected，才能解除阻塞继续往下走。
         */
        ZooKeeper.States state = zk.getState();
        switch (state) {
            case CONNECTING:
                System.out.println("ing ...");
                break;
            case ASSOCIATING:
                break;
            case CONNECTED:
                System.out.println("ed ...");
                break;
            case CONNECTEDREADONLY:
                break;
            case CLOSED:
                break;
            case AUTH_FAILED:
                break;
            case NOT_CONNECTED:
                break;
        }

        //创建节点 /ooxx/olddata
        String pathName = zk.create("/ooxx", "olddata".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        //取/ooxx节点的数据
        Stat stat = new Stat();
        byte[] node = zk.getData("/ooxx", new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println("getData watch: " + event.toString());
                try {
                    //true default watch 被重新注册，这个default是new zookeeper的时候传的那个watch
                    zk.getData("/ooxx", true, stat);
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }, stat);
        System.out.println(new String(node));

        //修改/ooxx节点数据
        //触发回调
        Stat stat1 = zk.setData("/ooxx", "newdata".getBytes(), 0);
        //还会触发吗？
        Stat stat2 = zk.setData("/ooxx", "newdata2".getBytes(), stat1.getVersion());

        /**
         * reactive模型，回调，原来代码的执行顺序会产生阻塞，会产生线程进入等待状态，cpu因为线程的等待而产生
         * 浪费，reactive异步的模型，是方法内容的缔造者，而不是逻辑执行顺序的缔造者，尽量减少线程阻塞、中断。
         */
        System.out.println("--------async start--------");
        zk.getData("/ooxx", false, new AsyncCallback.DataCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                System.out.println("--------async call back--------");
                System.out.println(ctx.toString());
                System.out.println(new String(data));
            }
        }, "abc");
        System.out.println("--------async over--------");

    }
}
