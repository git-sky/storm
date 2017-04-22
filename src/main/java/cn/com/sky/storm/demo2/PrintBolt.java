package cn.com.sky.storm.demo2;

import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class PrintBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 1L;

	private int indexId;

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		this.indexId = context.getThisTaskIndex();
	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {

		String rec = tuple.getString(0);
//		System.err.println("String recieved: " + rec);
		System.err.println(String.format("PrintBolt[%d] String recieved: %s", this.indexId, rec));


	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// do nothing
	}

}