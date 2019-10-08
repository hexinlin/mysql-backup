package com.hxl.mysql_backup.util;

import java.math.BigDecimal;
import java.util.Date;

import com.github.shyiko.mysql.binlog.event.deserialization.ColumnType;

public class ColumTypeParse {
	
	public final static int text = -4;

	
	
	public  ParseResult parse(int customType,Object obj) {
		 ColumnType type = ColumnType.byCode(customType);
		 Object result = null;
		 boolean isDigital = false;
		 if (type == null) {
	            try {
	            	
	            	if(null==obj) {
	            		return new ParseResult(false, null);
	            	}
	            	if(obj instanceof byte[]) {
	            		return new ParseResult(false, new String((byte[])obj));
	            	}else {
	            		throw new Exception("Unknown type !");
	            	}
					
					
	            	
				} catch (Exception e) {
					e.printStackTrace();
				}
	        }

	        switch (type) {
	            case DECIMAL:
	            case NEWDECIMAL:
	            	isDigital = true;
	            	result = (BigDecimal)obj;
	                break;
	            case TINY:
	            case SHORT:
	            case LONG:
	            case INT24:
	            	isDigital = true;
	            	result = (Integer)obj;
	            	break;
	            case LONGLONG:
	            	isDigital = true;
	            	result = (Long)obj;
	            	break;
	            case FLOAT:
	            	isDigital = true;
	            	result = (Float)obj;
	            	break;
	            case DOUBLE:
	            	isDigital = true;
	            	result = (Double)obj;
	            	break;
	            case DATE:
	            	result = (Date)obj;
	            	result = DateUtils.format((Date)result);
	                break;
	            case TIME:
	            case TIME_V2:
	            	result = (Date)obj;
	            	result = DateUtils.format((Date)result,"HH:mm:ss");
	                break;
	            case DATETIME:
	            case DATETIME_V2:
	            case TIMESTAMP:
	            case TIMESTAMP_V2:
	            	result = (Date)obj;
	            	result = DateUtils.format((Date)result);
	                break;
	            case VARCHAR:
	            	result = (String)obj;
	            	break;
	            default:
	            	result = (String)obj;
	        }
		return new ParseResult(isDigital, result);
	}
	
	
	public class ParseResult {
		private boolean isDigital;
		private Object obj;
		public ParseResult(boolean isDigital,Object obj) {
			this.isDigital = isDigital;
			this.obj = obj;
		}
		public boolean isDigital() {
			return isDigital;
		}
		public void setDigital(boolean isDigital) {
			this.isDigital = isDigital;
		}
		public Object getObj() {
			return obj;
		}
		public void setObj(Object obj) {
			this.obj = obj;
		}
		
		
	}
}
