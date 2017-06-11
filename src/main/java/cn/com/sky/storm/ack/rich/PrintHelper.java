package cn.com.sky.storm.ack.rich;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 日志工具类
 */
public class PrintHelper {

	private static SimpleDateFormat sf = new SimpleDateFormat("mm:ss:SSS");

	public static void print(String out) {
		System.err.println("["+sf.format(new Date()) + "] [" + Thread.currentThread().getName() + "] " + out);
	}

}