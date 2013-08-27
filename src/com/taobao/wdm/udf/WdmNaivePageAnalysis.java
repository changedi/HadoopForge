/**
 * 
 */
package com.taobao.wdm.udf;

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @author zunyuan.jy
 * 
 */
public class WdmNaivePageAnalysis extends UDF {

	public Double evaluate(String str) {
		if (str == null)
			return 0d;
		String[] strs = str.split("\\|\\|");
		Set<String> set = new HashSet<String>();
		for (String s : strs) {
			set.add(s);
		}
		return 1-(1.0 * set.size()) / strs.length;
	}

	public static void main(String[] args) {
		String s = "Page_Home||Page_Home>Page_Home>Page_Home:Page_Webview||Page_Home";
		WdmNaivePageAnalysis wt = new WdmNaivePageAnalysis();
		System.out.println(wt.evaluate(s));
	}
}
