package cn.com.sky.storm.demo3;

import java.text.SimpleDateFormat;
import java.util.Date;

public class PrintHelper {

	private static SimpleDateFormat sf = new SimpleDateFormat("mm:ss:SSS");

	public static void print(String out) {
		System.err.println("["+sf.format(new Date()) + "] [" + Thread.currentThread().getName() + "] " + out);
	}

}