package cn.com.sky.storm.demo3;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * 通常情况下，实现一个Bolt，可以实现IRichBolt接口或继承BaseRichBolt，如果不想自己处理结果反馈，可以实现IBasicBolt接口或继承BaseBasicBolt，
 * 它实际上相当于自动做掉了prepare方法和collector.emit.ack(inputTuple)；
 */
public class ExclaimRichBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	
	private OutputCollector collector;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple tuple) {
		// 第一个参数是传入Bolt的tuple，第二个参数是新产生的tuple的value，这种emit的方式，在Storm中称为: "anchor"。
		this.collector.emit(tuple, new Values(tuple.getString(0) + "!"));
		this.collector.ack(tuple);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("after_excl"));
	}

}