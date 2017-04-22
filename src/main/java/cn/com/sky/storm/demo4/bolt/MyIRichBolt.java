package cn.com.sky.storm.demo4.bolt;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class MyIRichBolt implements IRichBolt {

	private static final long serialVersionUID = 1L;

	private OutputCollector collector;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	/**
	 * execute接受一个tuple进行处理，并用prepare方法传入的OutputCollector的ack方法（表示成功）或fail（表示失败）来反馈处理结果
	 */
	@Override
	public void execute(Tuple input) {
		try {
			Object val = input.getValue(0);
			System.out.println(val);
			collector.ack(input);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * cleanup 同ISpout的close方法，在关闭前调用。同样不保证其一定执行。
	 */
	@Override
	public void cleanup() {

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
