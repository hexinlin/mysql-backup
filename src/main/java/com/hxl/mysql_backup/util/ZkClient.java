package com.hxl.mysql_backup.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.log4j.Logger;

public class ZkClient {

	private static String _zkUrl = null;
	private static CuratorFramework _client = null;
	private static String _zkPath = null;
	private static final Logger _logger = Logger.getLogger(ZkClient.class);
	public static boolean isLeader = false;
	private static CountDownLatch _latch = new CountDownLatch(1);
	
	static {
		Properties properties = new Properties();
        InputStream inputStream = KafkaSender.class.getClassLoader().getResourceAsStream("zookeeper.properties");
        try {
			properties.load(inputStream);
			_zkUrl = properties.getProperty("zkUrl");
			_zkPath = properties.getProperty("zkPath");
		} catch (IOException e) {
			_logger.error(e);
		}
		
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		_client = CuratorFrameworkFactory.newClient(_zkUrl, retryPolicy);
		_client.start();
	}
	//存储日志文件读取位置信息
	public static void UpdatePositition(String binlogFilename,Long binlogPosition,int num) throws Exception {
		
			_client.setData().forPath(_zkPath,(binlogFilename+"|"+binlogPosition+"|"+num).getBytes());
		
		
	}
	
	/**
	 * 获取当前存储的位置信息
	 * @return
	 */
	public static byte[] getPosition() {
		byte[] data = null;
		try {
			data = _client.getData().forPath(_zkPath);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return data;
	}
	
	
	//获取领导权，实现容灾
	public static void getLeader() {
		
		ExecutorService fixedThreadPool = Executors.newFixedThreadPool(1);
		
		fixedThreadPool.submit(new Runnable() {
			
			public void run() {
				LeaderSelectorListener listener  = new LeaderSelectorListenerAdapter() {
					
					public void takeLeadership(CuratorFramework client) throws Exception {
						_logger.info("获得领导权.");
						isLeader  = true;
						_latch.await();
						_logger.info("释放领导权");
					}
				};
				
				LeaderSelector selector = new LeaderSelector(_client, _zkPath+"/leader", listener);
				selector.autoRequeue();
				selector.start();
				
			}
		});
			
		}
}
