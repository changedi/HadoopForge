/**
 * 
 */
package com.taobao.wdm.udf;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @author zunyuan.jy
 * 
 * @since 2013-10-10
 */
public class MaxMinFind extends UDF {
	public String evaluate(String hist, int mode) {
		Map<String, Double> map = new HashMap<String, Double>();
		if (hist == null || hist.equals("")) {
			return "";
		}

		String[] splKVs = hist.split(",");
		for (String KV : splKVs) {
			String[] kv = KV.split(":");
			map.put(kv[0], Double.valueOf(kv[1]));
		}
		return find(map, mode);
	}

	private String find(Map<String, Double> map, int mode) {
		String p = "";
		if (mode == 0) {
			// find min
			double max = Double.MAX_VALUE;
			for (String key : map.keySet()) {
				Double val = map.get(key);
				if (val < max) {
					max = val;
					p = key + ":" + val;
				}
			}
		} else {
			// find max
			double min = 0;
			for (String key : map.keySet()) {
				Double val = map.get(key);
				if (val > min) {
					min = val;
					p = key + ":" + val;
				}
			}
		}
		return p;
	}

	public static void main(String[] args) {
		String s1 = "2013100800:3.46,2013100801:2.69,2013100802:0.38,2013100803:0.38,2013100805:1.15,2013100806:2.31,2013100807:2.31,2013100808:3.08,2013100809:5.38,2013100810:5.77,2013100811:6.15,2013100812:7.31,2013100813:6.15,2013100814:2.69,2013100815:6.15,2013100816:4.23,2013100817:2.69,2013100818:5.38,2013100819:9.62,2013100820:6.54,2013100821:6.92,2013100822:5.77,2013100823:3.46";
		String s2 = "2013100800:3.03,2013100801:1.64,2013100802:0.69,2013100803:0.44,2013100804:0.57,2013100805:0.51,2013100806:1.96,2013100807:2.84,2013100808:3.47,2013100809:4.29,2013100810:4.86,2013100811:5.49,2013100812:5.87,2013100813:6.06,2013100814:5.74,2013100815:5.11,2013100816:4.17,2013100817:4.8,2013100818:5.49,2013100819:6.38,2013100820:7.13,2013100821:7.26,2013100822:7.51,2013100823:4.67";
		MaxMinFind hdc = new MaxMinFind();
		System.out.println(hdc.evaluate(s1,0));
		System.out.println(hdc.evaluate(s2,1));
	}
}
