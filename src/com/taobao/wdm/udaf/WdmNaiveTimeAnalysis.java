/**
 * 
 */
package com.taobao.wdm.udaf;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;

/**
 * @author zunyuan.jy
 * 
 */
public class WdmNaiveTimeAnalysis extends UDAF {
	public static class Evaluator implements UDAFEvaluator {
		public static final double SAME_HOUR = 0.2;
		public static final double SAME_MINUTE = 0.8;
		private boolean empty;

		private class InnerObj {
			int hour;
			int min;
		}

		private List<InnerObj> list;
		private double result;

		public Evaluator() {
			super();
			init();
		}

		public void init() {
			result = 0;
			empty = true;
			list = new ArrayList<InnerObj>();
		}

		public boolean iterate(String str) {
			if (str != null) {
				InnerObj io = new InnerObj();
				io.hour = Integer.valueOf(str.split(">")[0].substring(6, 8));
				io.min = Integer.valueOf(str.split(">")[0].substring(8, 10));
				list.add(io);
				empty = false;
			}
			return true;
		}

		public List<InnerObj> terminatePartial() {
			// This is SQL standard - sum of zero items should be null.
			return empty ? null : list;
		}

		public boolean merge(List<InnerObj> s) {
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
				} else if(s.size()==4){
					Map<Integer,Integer> hourSet = new HashMap<Integer,Integer>();
					Map<Integer,Integer> minSet = new HashMap<Integer,Integer>();
					for(int i=0;i<s.size();i++){
						if(hourSet.containsKey(s.get(i).hour))
							hourSet.put(s.get(i).hour,hourSet.get(s.get(i).hour)+1);
						else
							hourSet.put(s.get(i).hour, 1);
						if(minSet.containsKey(s.get(i).min))
							minSet.put(s.get(i).min,minSet.get(s.get(i).min)+1);
						else
							minSet.put(s.get(i).min, 1);
					}
					if(hourSet.size()==1)
						result += SAME_HOUR * 1;
					else if(hourSet.size()==3)
						result += SAME_HOUR * 1/2;
					else if(hourSet.size()==2){
						if(hourSet.containsValue(3))
							result += SAME_HOUR * 3/4;
						else
							result += SAME_HOUR * 1;
					}
					if(minSet.size()==1)
						result += SAME_MINUTE * 1;
					else if(minSet.size()==3)
						result += SAME_MINUTE * 1/2;
					else if(minSet.size()==2){
						if(minSet.containsValue(3))
							result += SAME_MINUTE * 3/4;
						else
							result += SAME_MINUTE * 1;
					}
				}
				empty = false;
			}
			return true;
		}

		public DoubleWritable terminate() {
			// This is SQL standard - sum of zero items should be null.
			return empty ? null : new DoubleWritable(result);
		}
	}
}
