/**
 * 
 */
package com.taobao.wdm.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @author zunyuan.jy
 *
 * @since 2013-10-10
 */
public class DeepOffset extends UDF{
	public String evaluate(String max,String min, String stdMax, String stdMin) {
		String res = "";
		if(max==null || max.equals("")){
			return res;
		}
		if(min==null || min.equals("")){
			return res;
		}
		if(stdMax==null || stdMax.equals("")){
			return res;
		}
		if(stdMin==null || stdMin.equals("")){
			return res;
		}
		double thisMax = Double.valueOf(max);
		double thisMin = Double.valueOf(min);
		double thatMax = Double.valueOf(stdMax);
		double thatMin = Double.valueOf(stdMin);
		res = (thisMax-thatMax) + ":" + (thisMin-thatMin);
		return res;
	}

	public static void main(String[] args){
		String min = "2013100803";
		String max = "2013100822";
		String stdMin = "2013100801";
		String stdMax = "2013100823";
		DeepOffset hdc = new DeepOffset();
		System.out.println(hdc.evaluate(max, min, stdMax, stdMin));
	}
}
