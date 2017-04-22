package cn.com.sky.storm.demo2;

import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * <pre>
 * 
 * Bolt：
 * 在一个topology中接受数据然后执行处理的组件。
 * Bolt可以执行过滤、函数操作、合并、写数据库等任何操作。
 * Bolt是一个被动的角色，其接口中有个execute(Tuple input)函数,在接受到消息后会调用此函数，用户可以在其中执行自己想要的操作。
 * 
 * 
 * BaseBasicBolt会自动ack。
 */
public class ExclaimBasicBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 1L;

	private int indexId;

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		this.indexId = context.getThisTaskIndex();
	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String sentence = (String) tuple.getValue(0);
		String out = sentence + "!";
		System.err.println(String.format("ExclaimBasicBolt[%d] String recieved: %s", this.indexId, sentence));
		collector.emit(new Values(out));

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("excl_sentence"));
	}

}