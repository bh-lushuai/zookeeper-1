/*
 * Copyright 1999-2010 Alibaba.com All right reserved. This software is the confidential and proprietary information of
 * Alibaba.com ("Confidential Information"). You shall not disclose such Confidential Information and shall use it only
 * in accordance with the terms of the license agreement you entered into with Alibaba.com.
 */
package yangqi.code;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import yangqi.utils.PropertiesUtil;


/**
 * Executor maintains the ZooKeeper connection, and the class called the DataMonitor monitors the data in the ZooKeeper tree.
 * Also, Executor contains the main thread and contains the execution logic（DataMonitorListener）
 */
public class Executor implements Watcher, Runnable, DataMonitorListener {

    DataMonitor dm;
    ZooKeeper   zk;
    String desc;
    String   znode;
    boolean isFirst=true;

    String DEFAULT_HA_NODE="/hadoop-ha/ActiveBomb/active";
    static final String HADOOP_HA_CONFIG="hadoop_ha_monitor";

    public Executor(String hostPort, String znode,String desc) throws KeeperException, IOException {
        if(znode==null)znode=DEFAULT_HA_NODE;
        this.desc=desc;
        zk = new ZooKeeper(hostPort, 30000, this);
        dm = new DataMonitor(zk, znode, null, this);
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        Map config=PropertiesUtil.loadConfig(Executor.class.getResource("/monitor.yaml").getPath(), HashMap.class);
        try {
            ExecutorService service = Executors.newCachedThreadPool();
            //hadoop-ha monitor
            Set<Map.Entry<String,HashMap<String,String>>> hadoopConfig = ((Map) config.get(HADOOP_HA_CONFIG)).entrySet();
           for (Map.Entry<String,HashMap<String,String>> hadoop:hadoopConfig) {
              String desc= hadoop.getKey();
              HashMap<String, String> value = hadoop.getValue();
              String zkServer=value.get("zookeeper.server");
              String haNodePath=value.get("ha.node.path");
              service.execute(new Executor(zkServer,haNodePath,desc));
           }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /***************************************************************************
     * We do process any events ourselves, we just need to forward them on.
     * 
     * @see org.apache.zookeeper.Watcher#(org.apache.zookeeper.proto.WatcherEvent)
     */
    public void process(WatchedEvent event) {
        dm.process(event);
    }

    /**
     * the main thread long run
     */
    public void run() {
        try {
            synchronized (this) {
                while (!dm.dead) {
                    System.out.println(String.format("===========%s EXECUTOR START TO WAIT===========",desc));
                    wait();
                    System.out.println(String.format("===========%s EXECUTOR STOP WAIT===========",desc));
                }
            }
        } catch (InterruptedException e) {
        }
    }

    public void closingExtra(int rc) {
        synchronized (this) {
            notifyAll();
        }
    }


    public void existsExtra(byte[] data) {
        if (data == null) {
            // TODO send msg: no active namenode alive
        } else {
            String con=new String(data);
            if(isFirst){
                // TODO send msg
                System.out.println(String.format("monitor %s-namenode-ha acitive node  started,active namenode:%s",desc,con));
                return;
            }
            //TODO send msg　: namenoce switch
            System.out.println(String.format("%s-namenoce switch  started,active namenode:%s",desc,con));
        }
    }
}
