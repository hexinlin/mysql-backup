package com.hxl.mysql_backup;


import java.io.InputStream;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.FormatDescriptionEventData;
import com.github.shyiko.mysql.binlog.event.GtidEventData;
import com.github.shyiko.mysql.binlog.event.PreviousGtidSetEventData;
import com.github.shyiko.mysql.binlog.event.QueryEventData;
import com.github.shyiko.mysql.binlog.event.RotateEventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import com.github.shyiko.mysql.binlog.event.XidEventData;
import com.hxl.mysql_backup.domain.BinlogEntry;
import com.hxl.mysql_backup.domain.TableSchema;
import com.hxl.mysql_backup.util.ColumTypeParse;
import com.hxl.mysql_backup.util.ColumTypeParse.ParseResult;
import com.hxl.mysql_backup.util.MySqlUtils;
import com.hxl.mysql_backup.util.SendTaskUtil;
import com.hxl.mysql_backup.util.ZkClient;


public class BinlogParse {

	private final static Logger logger = Logger.getLogger(BinlogParse.class);
	private  static String host = null;
	private  static Integer port = null;
	private  static String username = null;
	private  static String password = null;
	private static String currentBinlogFileName="";
	private static long currentBinlogPosition=0;
	private static int currentSqlNum = 0;
	
	private static String initBinlogFileName="";
	private static long initBinlogPosition=0;
	private static int initSqlNum = 0;
	
	private static StringBuilder sb = new StringBuilder();
	private static String database = null;
	private static Map<Long,String> tableNameMap = new HashMap<Long,String>();
	private static Map<String,String[]> tableColumnMap = new HashMap<String, String[]>();
	private static Map<String,Integer> tableColumnTypeMap = new HashMap<String, Integer>();
	private static long tableId = 0;
	private static Map<String,Object> initParams = new HashMap<String, Object>();
	private static Map<String,List<TableSchema>> schemaParams = new HashMap<String, List<TableSchema>>();
	private static LinkedList<BinlogEntry> queue = new LinkedList<BinlogEntry>();
	private static long sqlMaxWait=5000l;
	private final static int _unkownType = -99;
	private static boolean _exceptionFlag = false;
	private static long time1 ;
	static {
		/*tableNameMap.put(33l, "t2.`user`");
		tableColumnMap.put(33l, new String[] {"`id`","`name`","`password`"});
		tableColumnTypeMap.put("33-0", 3);
		tableColumnTypeMap.put("33-1", 15); 
		tableColumnTypeMap.put("33-2", 15);
		
		tableNameMap.put(41l, "t2.`user`");
		tableColumnMap.put(41l, new String[] {"`id`","`name`","`password`"});
		tableColumnTypeMap.put("41-0", 3);
		tableColumnTypeMap.put("41-1", 15);
		tableColumnTypeMap.put("41-2", 15);*/
		try {
			ZkClient.getLeader();
			initParams();
			updateSchema();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) throws Exception {
		while(!ZkClient.isLeader) {
			logger.info("未获得领导权");
			TimeUnit.SECONDS.sleep(5);
		}
		final BinaryLogClient client = new BinaryLogClient(host, port, username, password);
		
		byte[] data = ZkClient.getPosition();
		//data = new byte[0];
		if(data.length==0) {
			currentBinlogFileName = (String)initParams.get("binlogFileName");
			if(null!=currentBinlogFileName&&currentBinlogFileName.length()>0) {
				currentBinlogPosition = Long.parseLong((String)initParams.get("binlogPosition"));
				client.setBinlogFilename(currentBinlogFileName);
				client.setBinlogPosition(currentBinlogPosition);
			}
			
		}else {
			String[] binPosition = new String(data).split("\\|");
			currentBinlogFileName = binPosition[0];
			currentBinlogPosition = Long.parseLong(binPosition[1]);
			//currentSqlNum = Integer.parseInt(binPosition[2]);
			
			
			initBinlogFileName = binPosition[0];
			initBinlogPosition = Long.parseLong(binPosition[1]);
			initSqlNum = Integer.parseInt(binPosition[2]);
			
			client.setBinlogFilename(currentBinlogFileName);
			client.setBinlogPosition(currentBinlogPosition);
		}
		
		SendTaskUtil.monitorQueue(queue,sqlMaxWait);
		logger.info("起始监听文件为："+currentBinlogFileName+",起始监听位置为："+currentBinlogPosition);
		time1 =System.nanoTime();
		client.registerEventListener(new BinaryLogClient.EventListener() {
			BinlogEntry binLogEntry = null;
			public void onEvent(Event event) {
			
				
				logger.info(event.getData());
				System.out.println("-------------------------------");
				
				if(_exceptionFlag) {
					return ;
				}
				Object data = event.getData();
			
				if(null==binLogEntry) {
					binLogEntry = new BinlogEntry();
					binLogEntry.setBinlogFilename(currentBinlogFileName);
					binLogEntry.setBinlogPosition(currentBinlogPosition);
				}
				
				if(data instanceof RotateEventData) {
					currentBinlogFileName = ((RotateEventData) data).getBinlogFilename();
					currentBinlogPosition = ((RotateEventData) data).getBinlogPosition();
					
					binLogEntry.setBinlogFilename(currentBinlogFileName);
					binLogEntry.setBinlogPosition(currentBinlogPosition);
					/*
					if(null!=binLogEntry) {
					 if(skip) {
								skip = false;
							}else {
								queue.put(binLogEntry);
							}
							
						
					}*/
					
				}else if(data instanceof FormatDescriptionEventData) {
					logger.debug(data.toString());
				}else if(data instanceof PreviousGtidSetEventData){
					logger.debug(data.toString());
				}else if(data instanceof GtidEventData){
					binLogEntry.setGtid(((GtidEventData) data).getGtid());
					currentSqlNum = Integer.parseInt(binLogEntry.getGtid().split(":")[1]);
				} else if(data instanceof QueryEventData) {
					database = ((QueryEventData) data).getDatabase();
					sb.append(((QueryEventData) data).getSql()).append("; ");
					
					if(!((QueryEventData) data).getSql().equalsIgnoreCase("begin")) {
						updateSchema();
						if(((QueryEventData) data).getSql().indexOf("wxrecord0016")>-1) {
							System.out.println(":"+(System.nanoTime()-time1)/1000000);
						}
						binLogEntry.setSql("USE `"+database+"`;"+sb.toString());
						binLogEntry.setNum(currentSqlNum);
						
						if(currentSqlNum<=initSqlNum) {
							
						}else {
							queue.offer(binLogEntry);
						}
						
						//System.out.println(binLogEntry.getGtid()+"|"+binLogEntry.getSql());
						binLogEntry = null;
						sb.delete(0, sb.length());
					}
					
				}else if(data instanceof TableMapEventData) {
					if(null==tableNameMap.get(((TableMapEventData) data).getTableId())) {
						tableNameMap.put(((TableMapEventData) data).getTableId(), "`"+((TableMapEventData) data).getDatabase()+"`.`"+ ((TableMapEventData) data).getTable()+"`");
					}
					
				}else if(data instanceof WriteRowsEventData) {
					
					
					
					tableId = ((WriteRowsEventData) data).getTableId();
					sb.append("insert into ").append(tableNameMap.get(tableId));
					//BitSet set = ((WriteRowsEventData) data).getIncludedColumns();
					sb.append(" values(");
					
					
					List<Serializable[]> list = ((WriteRowsEventData) data).getRows();
					Serializable[] serial = null;
					for(int j=0;j<list.size();j++) {
						serial = list.get(j);
						if(j!=0) {
							sb.append(",(");
						}
						for(int i=0;i<serial.length;i++) {
							
							int type = tableColumnTypeMap.get(tableNameMap.get(tableId)+"-"+i);
							ParseResult result = new ColumTypeParse().parse(type, serial[i]);
							if(null==serial[i]) {
								sb.append(serial[i]);
							}else {
								if(result.isDigital()) {
									sb.append(result.getObj());
								}else {
									sb.append("'").append(MySqlUtils.escape(result.getObj().toString())).append("'");
								}
							}
							
							if(i!=serial.length-1) {
								sb.append(",");
							}else {
								sb.append(") ");
							}
							
						}
					}
					sb.append("; ");
					
				}else if(data instanceof UpdateRowsEventData) {
					tableId = ((UpdateRowsEventData) data).getTableId();
					sb.append("update ").append(tableNameMap.get(tableId));
					sb.append(" set ");
					
					List<Map.Entry<Serializable[], Serializable[]>> list= ((UpdateRowsEventData) data).getRows();
					Map.Entry<Serializable[], Serializable[]> entry = list.get(0);
					Serializable[]  before = entry.getKey();
					Serializable[] after = entry.getValue();
					
					for(int i=0;i<after.length;i++) {
						int type = tableColumnTypeMap.get(tableNameMap.get(tableId)+"-"+i);
						
						ParseResult result = new ColumTypeParse().parse(type, after[i]);
						
						if(null==after[i]) {
							sb.append(tableColumnMap.get(tableNameMap.get(tableId))[i]).append("=").append(after[i]);
						}else {
							if(result.isDigital()) {
								sb.append(tableColumnMap.get(tableNameMap.get(tableId))[i]).append("=").append(result.getObj());
							}else {
								sb.append(tableColumnMap.get(tableNameMap.get(tableId))[i]).append("='").append(MySqlUtils.escape(result.getObj().toString())).append("'");
							}
						}
						
						
						
						if(i!=after.length-1) {
							sb.append(",");
						}

					}
					
					if(before.length>0) {
						sb.append(" where ");
					}
					
					for(int i=0;i<before.length;i++) {
						int type = tableColumnTypeMap.get(tableNameMap.get(tableId)+"-"+i);
						ParseResult result = new ColumTypeParse().parse(type, before[i]);
						
						if(null==before[i]) {
							sb.append(tableColumnMap.get(tableNameMap.get(tableId))[i]).append(" is ").append(before[i]);
						}else {
							if(result.isDigital()) {
								sb.append(tableColumnMap.get(tableNameMap.get(tableId))[i]).append("=").append(result.getObj());
							}else {
								sb.append(tableColumnMap.get(tableNameMap.get(tableId))[i]).append("='").append(MySqlUtils.escape(result.getObj().toString())).append("'");
							}
						}
						
						
						if(i!=after.length-1) {
							sb.append(" and ");
						}

					}
					
					sb.append(" ; ");
					
				}else if(data instanceof DeleteRowsEventData) {
					tableId = ((DeleteRowsEventData) data).getTableId();
					List<Serializable[]> list = ((DeleteRowsEventData) data).getRows();
					for(Serializable[] serials : list) {
						sb.append("delete from ").append(tableNameMap.get(tableId));
						if(serials.length>0) {
							sb.append(" where ");
							for(int i=0;i<serials.length;i++) {
								int type = tableColumnTypeMap.get(tableNameMap.get(tableId)+"-"+i);
								ParseResult result = new ColumTypeParse().parse(type, serials[i]);
								if(null==serials[i]) {
									sb.append(tableColumnMap.get(tableNameMap.get(tableId))[i]).append(" is ").append(serials[i]);
								}else {
									if(result.isDigital()) {
										sb.append(tableColumnMap.get(tableNameMap.get(tableId))[i]).append("=").append(result.isDigital());
									}else {
										sb.append(tableColumnMap.get(tableNameMap.get(tableId))[i]).append("='").append(MySqlUtils.escape(result.getObj().toString())).append("'");
									}
								}
								
							
								if(i!=serials.length-1) {
									sb.append(" and ");
								}
							}
						}
						sb.append(";");
					}
				} else if(data instanceof XidEventData) {
					sb.append("COMMIT; ");
					binLogEntry.setSql(sb.toString());
					binLogEntry.setNum(currentSqlNum);
					if(currentSqlNum<=initSqlNum) {
						
					}else {
						queue.offer(binLogEntry);
					}
					//System.out.println(binLogEntry.getGtid()+"|"+binLogEntry.getSql());
					binLogEntry = null;
					sb.delete(0, sb.length());
				}
			}
		});
		client.connect();
	}
	
	
	public static boolean initParams() throws Exception {
		InputStream inputStream = BinlogParse.class.getClassLoader().getResourceAsStream("app.properties");
		Properties prop = new Properties();
		prop.load(inputStream);
		Iterator<Object> itr = prop.keySet().iterator();
		Object key = null;
		while(itr.hasNext()) {
			key = itr.next();
			initParams.put((String)key, prop.get(key));
		}
		
		host = (String)initParams.get("mysql.host");
		port = Integer.parseInt((String)initParams.get("mysql.port"));
		username = (String)initParams.get("mysql.username");
		password = (String)initParams.get("mysql.password");
		
		sqlMaxWait = Long.parseLong((String)initParams.get("sql.maxwait"));
		
		return true;
	}
	
	public static boolean updateSchema(){
		  String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
		  String DB_URL = "jdbc:mysql://"+host+":"+port+"/";
		  Connection conn = null;
	      Statement stmt = null;
	        try{
	            // 注册 JDBC 驱动
	            Class.forName(JDBC_DRIVER);
	        
	            // 打开链接
	            logger.debug("连接数据库...");
	            conn = DriverManager.getConnection(DB_URL,username,password);
	        
	            stmt = conn.createStatement();
	            String sql;
	            sql = "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION ,DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS " +
	             " "+
	            		"  ORDER BY TABLE_SCHEMA,TABLE_NAME,ORDINAL_POSITION";
	            ResultSet rs = stmt.executeQuery(sql);
	        
	            String key = null;
	            // 展开结果集数据库
	            while(rs.next()){
	            	key = "`"+rs.getString("TABLE_SCHEMA")+"`.`"+rs.getString("TABLE_NAME")+"`";
	            	if(null==schemaParams.get(key)) {
	            		schemaParams.put(key, new ArrayList<TableSchema>());
	            	}
	            	schemaParams.get(key).add(new TableSchema(rs.getString("TABLE_SCHEMA"), rs.getString("TABLE_NAME"), rs.getString("COLUMN_NAME"), rs.getInt("ORDINAL_POSITION")-1, rs.getString("DATA_TYPE")));
	            }
	            // 完成后关闭
	            rs.close();
	            stmt.close();
	            conn.close();
	        }catch(SQLException se){
	            // 处理 JDBC 错误
	            se.printStackTrace();
	        }catch(Exception e){
	            // 处理 Class.forName 错误
	            e.printStackTrace();
	        }finally{
	            // 关闭资源
	            try{
	                if(stmt!=null) stmt.close();
	            }catch(SQLException se2){
	            }
	            try{
	                if(conn!=null) conn.close();
	            }catch(SQLException se){
	                se.printStackTrace();
	            }
	            logger.debug("关闭数据库...");
	        }
	        
	        Set<Entry<String, List<TableSchema>>>  set  =schemaParams.entrySet();
	        Iterator<Entry<String, List<TableSchema>>> itr = set.iterator(); 
	        Entry<String, List<TableSchema>> entry = null;
	        tableColumnMap.clear();
	        tableColumnTypeMap.clear();
	        List<TableSchema> list = null;
	        //String[] columNames = null;
	        while(itr.hasNext()) {
	        	entry = itr.next();
	        	tableColumnMap.put(entry.getKey(), new String[entry.getValue().size()]);
	        	list = entry.getValue();
	        	//columNames = new String[list.size()];
	        	for(int i=0;i<list.size();i++) {
	        		//columNames[i] = list.get(i).getColumName();
	        		tableColumnMap.get(entry.getKey())[i] = list.get(i).getColumName();
	        		tableColumnTypeMap.put(entry.getKey()+"-"+i, list.get(i).getDataTypeNum());
	        		if(list.get(i).getDataTypeNum()==_unkownType) {
	        		  //  _exceptionFlag = true;
	        			//logger.error("unkown dataType:"+list.get(i).getSchemaName()+"."+list.get(i).getTableName()+"."+list.get(i).getColumName()+"."+list.get(i).getDataType());
	        		}
	        	}
	        }
	        
	      
	       return true;
	}
}
