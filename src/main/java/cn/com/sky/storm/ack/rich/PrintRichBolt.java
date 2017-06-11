package cn.com.sky.storm.ack.rich;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;

public class PrintRichBolt extends BaseRichBolt {

	private OutputCollector collector;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;

	}

	@Override
	public void execute(Tuple input) {
		
		System.out.println("==============execute=====begin====================");
//		System.out.println(input.getStringByField("sentence"));
//		System.out.println(input.getValueByField("sentence"));
//		System.out.println(input.getFields());
		
		System.out.println(input.getStringByField("after_excl"));
		System.out.println(input.getValueByField("after_excl"));
		System.out.println(input.getFields());
		System.out.println("=============execute=======end===================");
		
		String rec = input.getString(0);
		Utils.sleep(5000);
		PrintHelper.print("rec: " + rec);
		collector.ack(input);//rich实现类，必须ack才能被追踪

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}

}