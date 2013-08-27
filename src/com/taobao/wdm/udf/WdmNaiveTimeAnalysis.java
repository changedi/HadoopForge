/**
 * 
 */
package com.taobao.wdm.udf;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @author zunyuan.jy
 * 
 */
public class WdmNaiveTimeAnalysis extends UDF {
	public static final double SAME_HOUR = 0.2;
	public static final double SAME_MINUTE = 0.8;

	public Double evaluate(String str) {
		if (str == null)
			return 0d;
		String[] strs = str.split("\\|\\|");
		List<InnerObj> list = new ArrayList<InnerObj>();
		for (String s : strs) {
			if (s != null) {
				InnerObj io = new InnerObj();
				io.hour = Integer.valueOf(s.split(">")[0].substring(8, 10));
				io.min = Integer.valueOf(s.split(">")[0].substring(10, 12));
				list.add(io);
			}
		}
		return merge(list);
	}

	private class InnerObj {
		int hour;
		int min;
	}

	private double merge(List<InnerObj> s) {
		double result = 0;
		if (s != null) {
			if (s.size() == 2) {
				if (s.get(0).hour == s.get(1).hour)
					result += SAME_HOUR * 1;
				if (s.get(0).min == s.get(1).min)
					result += SAME_MINUTE * 1;
			} else if (s.size() == 3) {
				if (s.get(0).hour == s.get(1).hour
						&& s.get(1).hour == s.get(2).hour)
					result += SAME_HOUR * 1;
				else if (s.get(0).hour == s.get(1).hour
						|| s.get(0).hour == s.get(2).hour
						|| s.get(2).hour == s.get(1).hour)
					result += SAME_HOUR * 2 / 3;
				if (s.get(0).min == s.get(1).min
						&& s.get(1).min == s.get(2).min)
					result += SAME_MINUTE * 1;
				else if (s.get(0).min == s.get(1).min
						|| s.get(0).min == s.get(2).min
						|| s.get(2).min == s.get(1).min)
					result += SAME_MINUTE * 2 / 3;
			} else if (s.size() == 4) {
				Map<Integer, Integer> hourSet = new HashMap<Integer, Integer>();
				Map<Integer, Integer> minSet = new HashMap<Integer, Integer>();
				for (int i = 0; i < s.size(); i++) {
					if (hourSet.containsKey(s.get(i).hour))
						hourSet.put(s.get(i).hour,
								hourSet.get(s.get(i).hour) + 1);
					else
						hourSet.put(s.get(i).hour, 1);
					if (minSet.containsKey(s.get(i).min))
						minSet.put(s.get(i).min, minSet.get(s.get(i).min) + 1);
					else
						minSet.put(s.get(i).min, 1);
				}
				if (hourSet.size() == 1)
					result += SAME_HOUR * 1;
				else if (hourSet.size() == 3)
					result += SAME_HOUR * 1 / 2;
				else if (hourSet.size() == 2) {
					if (hourSet.containsValue(3))
						result += SAME_HOUR * 3 / 4;
					else
						result += SAME_HOUR * 1;
				}
				if (minSet.size() == 1)
					result += SAME_MINUTE * 1;
				else if (minSet.size() == 3)
					result += SAME_MINUTE * 1 / 2;
				else if (minSet.size() == 2) {
					if (minSet.containsValue(3))
						result += SAME_MINUTE * 3 / 4;
					else
						result += SAME_MINUTE * 1;
				}
			}
		}
		return result;
	}

	public static void main(String[] args){
		String s = "20130816212334>20130816212336||20130817160118>20130817160130||20130818210329>20130818210500";
		WdmNaiveTimeAnalysis wt = new WdmNaiveTimeAnalysis();
		System.out.println(wt.evaluate(s));
	}
}
