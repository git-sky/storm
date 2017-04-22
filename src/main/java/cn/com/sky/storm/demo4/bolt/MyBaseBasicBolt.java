package cn.com.sky.storm.demo4.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

/**
 * <pre>
 * 
 * 通常情况下，实现一个Bolt，可以实现IRichBolt接口或继承BaseRichBolt，
 * 如果不想自己处理结果反馈，可以实现IBasicBolt接口或继承BaseBasicBolt，
 * 它实际上相当于自动做掉了prepare方法和collector.emit.ack(inputTuple)；
 * 
 * </pre>
 */
public class MyBaseBasicBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 1L;

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		try {
			Object val = input.getValue(0);
			System.out.println(val);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

}
