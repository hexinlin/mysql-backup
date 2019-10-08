package com.hxl.mysql_backup.util;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.hxl.mysql_backup.domain.BinlogEntry;

public class SendTaskUtil {

	private final static Logger _logger = Logger.getLogger(SendTaskUtil.class);
	private final static ScheduledExecutorService _scheduledThreadPool = Executors
			.newScheduledThreadPool(1);

	public static void monitorQueue(final LinkedList<BinlogEntry> queue,
			long sqlMaxWait) {

		_scheduledThreadPool.scheduleWithFixedDelay(new Runnable() {

			public void run() {
				BinlogEntry[] entries = null;
				int size = queue.size();
				if (size > 0) {
					entries = new BinlogEntry[size];
					for (int i = 0; i < size; i++) {
						entries[i] = queue.get(i);
					}
					// 执行发送任务
					_logger.info("执行发送任务，start-BinlogFilename："
							+ entries[0].getBinlogFilename()
							+ ",start-BinlogPosition:"
							+ entries[0].getBinlogPosition()
							+ ",sqlNum:"
							+ entries[0].getNum()
							+ "，end-BinlogFilename："
							+ entries[entries.length - 1].getBinlogFilename()
							+ ",end-BinlogPosition:"
							+ entries[entries.length - 1].getBinlogPosition()
							+ ",sqlnum:"
							+ entries[entries.length - 1].getNum());
					
					CountDownLatch latch = new CountDownLatch(entries.length);
					KafkaSender.produce(entries, latch);
					// 更新zookeeper节点
					try {
						ZkClient.UpdatePositition(entries[entries.length - 1].getBinlogFilename(),
								entries[entries.length - 1].getBinlogPosition(), entries[entries.length - 1].getNum());
						for(BinlogEntry entry : entries) {
							queue.remove(entry);
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
					
				}else {
					_logger.info("执行发送任务，队列中无数据。");
				}

			}
		}, sqlMaxWait, sqlMaxWait, TimeUnit.MILLISECONDS);

	}

}
