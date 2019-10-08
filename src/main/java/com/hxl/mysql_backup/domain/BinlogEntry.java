package com.hxl.mysql_backup.domain;


public class BinlogEntry {

	private String gtid;
	private String binlogFilename;
	private Long binlogPosition;
	private String sql;
	private Integer num;//同一binlog中的第几个sql
	
	
	public String getGtid() {
		return gtid;
	}
	public void setGtid(String gtid) {
		this.gtid = gtid;
	}
	public String getBinlogFilename() {
		return binlogFilename;
	}
	public void setBinlogFilename(String binlogFilename) {
		this.binlogFilename = binlogFilename;
	}
	public Long getBinlogPosition() {
		return binlogPosition;
	}
	public void setBinlogPosition(Long binlogPosition) {
		this.binlogPosition = binlogPosition;
	}
	public String getSql() {
		return sql;
	}
	public void setSql(String sql) {
		this.sql = sql;
	}
	public Integer getNum() {
		return num;
	}
	public void setNum(Integer num) {
		this.num = num;
	}
	
	
	
}
