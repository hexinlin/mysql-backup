package com.hxl.mysql_backup.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;

import com.hxl.mysql_backup.domain.BinlogEntry;


public class KafkaSender {
	   
	    private static KafkaProducer<String, String> producer = null;
	    private static String TOPIC = null;
	    private final static Logger logger = Logger.getLogger(KafkaSender.class);
	   
	    static {
	    	Properties properties = new Properties();
	        InputStream inputStream = KafkaSender.class.getClassLoader().getResourceAsStream("kafka.properties");
	        try {
				properties.load(inputStream);
				TOPIC = properties.getProperty("binlong.topic");
				properties.remove("binlong.topic");
			} catch (IOException e) {
				e.printStackTrace();
				logger.error(e);
			}
	        producer = new KafkaProducer<String, String>(properties);
	    }
	    
	   
	    
	   public static void produce(BinlogEntry[] entries,final CountDownLatch latch) {
		   String str = null;
		   for(BinlogEntry entry:entries) {
			    str = entry.getGtid()+"|"+entry.getSql();
			   // logger.info("发送消息："+str);
	        	ProducerRecord<String, String> record = new ProducerRecord<String, String>(TOPIC, str);
	        	producer.send(record,new Callback() {
					
					public void onCompletion(RecordMetadata metadata, Exception exception) {
						latch.countDown();
					}
				});
	        }
	        
	    }
	  
	
	
}
