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
public class DeepDiff extends UDF{
	public Double evaluate(String max, String min) {
		double res = 0d;
		if(max==null || max.equals("")){
			return 0d;
		}
		if(min==null || min.equals("")){
			return 0d;
		}
		String[] stdKVs = max.split(":");
		double thisMax = Double.valueOf(stdKVs[1]);
		
		String[] splKVs = min.split(":");
		double thisMin = Double.valueOf(splKVs[1]);
		res = thisMax/thisMin;
		return res;
	}

	public static void main(String[] args){
		String min = "2013100803:0.38";
		String max = "2013100822:7.51";
		DeepDiff hdc = new DeepDiff();
		System.out.println(hdc.evaluate(max, min));
	}
}
