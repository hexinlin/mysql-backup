package com.hxl.mysql_backup.util;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;

public class ZookeeperClient {

	private static final String zkUrl = "192.168.1.244:2181";
	private static CuratorFramework _client = null;
	private static final Logger _logger = Logger.getLogger(ZookeeperClient.class);
	
	static {
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		_client = CuratorFrameworkFactory.newClient(zkUrl, retryPolicy);
		_client.start();
	}
	
	public static void main(String[] args) throws Exception{
		//1.持久化节点
		byte[] data = {1,2,3};
		_client.create().withMode(CreateMode.PERSISTENT).forPath("/zktest/p1",data);
		
		//2.临时节点
		//CuratorFrameworkFactory.newClient(connectString, sessionTimeoutMs, connectionTimeoutMs, retryPolicy);
		//_client.create().withMode(CreateMode.EPHEMERAL).forPath("/zktest/e1",data);
		
		//3.持久化时序节点
		//_client.create().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath("/zktest/ps1",data);
		
		//4.临时时序节点
		//_client.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath("/zktest/es1",data);
		
		//5.设置和获取节点数据
		//_client.setData().forPath("/zktest/ps1",data);
		//byte[] data2 = _client.getData().forPath("/zktest/ps1");
		
		//6.分布式锁
		/*ExecutorService fixedThreadPool = Executors.newFixedThreadPool(5);
	    for (int i = 0; i < 5; i++) {
	        fixedThreadPool.submit(new Runnable() {
	            public void run() {

	                while (true) {
	                    try {
	                    	doDistributedWork();
	                    } catch (Exception e) {
	                        e.printStackTrace();
	                    }
	                }
	            }
	        });
	    }
	    */
	    //7.LeaderSelector实现选举
	    //leaderSelector();
	    
	    //8.LeaderLatch实现选举
	   // leaderSelector2();
	}
	
	//分布式锁
	public static void doDistributedWork()  throws Exception{
		InterProcessMutex ipm = new InterProcessMutex(_client, "/zktest/distributed_lock");
		try {
			ipm.acquire();
			_logger.info("Thread ID:" + Thread.currentThread().getId() + " acquire the lock");
			Thread.sleep(1000);
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			ipm.release();
			_logger.info("Thread ID:" + Thread.currentThread().getId() + " release the lock");
		}
	}
	//选举
	public static void leaderSelector() {
		LeaderSelectorListener listener  = new LeaderSelectorListenerAdapter() {
			
			public void takeLeadership(CuratorFramework client) throws Exception {
				_logger.info("获得领导权.");
				Thread.sleep(10000);
				_logger.info("释放领导权");
			}
		};
		
		LeaderSelector selector = new LeaderSelector(_client, "/zktest/leader", listener);
		selector.autoRequeue();
		selector.start();
	}
	
	//选举
	public static void leaderSelector2() throws Exception {
		LeaderLatch leader = new LeaderLatch(_client, "/zktest/leader");
		leader.addListener(new LeaderLatchListener() {
			
			public void notLeader() {
				_logger.info("未获得领导权");
				
			}
			
			public void isLeader() {
				_logger.info("获得领导权");
				try {
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				_logger.info("放弃领导权");
				
			}
		});
		leader.start();
	}
}
