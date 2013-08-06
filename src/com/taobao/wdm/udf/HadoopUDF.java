/**
 * 
 */
package com.taobao.wdm.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @author zunyuan.jy
 * 
 */
public class HadoopUDF extends UDF {
	public Integer evaluate(Integer a, Integer b) {

		if (null == a || null == b) {

			return null;

		}
		return a + b;
	}
}
