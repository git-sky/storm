package cn.com.sky.storm.demo4.spout;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;

public class MyBaseRichSpout extends BaseRichSpout {

	private static final long serialVersionUID = 1L;

	/**
	 * open方法是初始化动作。允许你在该spout初始化时做一些动作，传入了上下文，方便取上下文的一些数据。
	 * 
	 */
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		// TODO Auto-generated method stub

	}

	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}

}
