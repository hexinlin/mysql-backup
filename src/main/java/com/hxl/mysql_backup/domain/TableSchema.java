package com.hxl.mysql_backup.domain;

import com.github.shyiko.mysql.binlog.event.deserialization.ColumnType;
import com.hxl.mysql_backup.util.ColumTypeParse;

public class TableSchema {

	private String schemaName;
	private String tableName;
	private String columName;
	private int position;
	private String dataType;
	private int dataTypeNum;
	
	
	public TableSchema(String schemaName,String tableName,String columName,int position,String dataType) {
		this.schemaName = schemaName;
		this.tableName = tableName;
		this.columName = columName;
		this.position = position;
		this.dataType = dataType;
	}


	public String getSchemaName() {
		return schemaName;
	}


	public void setSchemaName(String schemaName) {
		this.schemaName = schemaName;
	}


	public String getTableName() {
		return tableName;
	}


	public void setTableName(String tableName) {
		this.tableName = tableName;
	}


	public String getColumName() {
		return columName;
	}


	public void setColumName(String columName) {
		this.columName = columName;
	}


	public int getPosition() {
		return position;
	}


	public void setPosition(int position) {
		this.position = position;
	}


	public String getDataType() {
		return dataType;
	}


	public void setDataType(String dataType) {
		this.dataType = dataType;
	}


	public int getDataTypeNum() {
		if(this.dataType.equalsIgnoreCase("decimal")) {
			this.dataTypeNum=0;
		}else if(this.dataType.equalsIgnoreCase("tinyint")) {
			this.dataTypeNum=1;
		}else if(this.dataType.equalsIgnoreCase("smallint")) {
			this.dataTypeNum=2;
		}else if(this.dataType.equalsIgnoreCase("int")) {
			this.dataTypeNum=3;
		}else if(this.dataType.equalsIgnoreCase("float")) {
			this.dataTypeNum=4;
		}else if(this.dataType.equalsIgnoreCase("double")) {
			this.dataTypeNum=5;
		}else if(this.dataType.equalsIgnoreCase("timestamp")) {
			this.dataTypeNum=7;
		}else if(this.dataType.equalsIgnoreCase("bigint")) {
			this.dataTypeNum=8;
		}else if(this.dataType.equalsIgnoreCase("mediumint")) {
			this.dataTypeNum=9;
		}else if(this.dataType.equalsIgnoreCase("date")) {
			this.dataTypeNum=10;
		}else if(this.dataType.equalsIgnoreCase("time")) {
			this.dataTypeNum=11;
		}else if(this.dataType.equalsIgnoreCase("datetime")){
			this.dataTypeNum=12;
		}else if(this.dataType.equalsIgnoreCase("year")){
			this.dataTypeNum=13;
		}else if(this.dataType.equalsIgnoreCase("varchar")||this.dataType.equalsIgnoreCase("char")){
			this.dataTypeNum=15;
		}else if(this.dataType.equalsIgnoreCase("bit")){
			this.dataTypeNum=16;
		}else if(this.dataType.indexOf("text")>-1){
			this.dataTypeNum=ColumTypeParse.text;
		}else {
			this.dataTypeNum=-99;
		}
		return dataTypeNum;
	}


	
	
}
