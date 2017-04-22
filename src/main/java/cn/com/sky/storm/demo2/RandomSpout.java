package cn.com.sky.storm.demo2;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * <pre>
 * 
 * 该Spout代码里面最核心的部分有两个：
 * 
 * a. 用collector.emit()方法发射tuple。我们不用自己实现tuple，我们只需要定义tuple的value，Storm会帮我们生成tuple。Values对象接受变长参数。
 * Tuple中以List存放Values，List的Index按照new Values(obj1, obj2,...)的参数的index,
 * 例如我们emit(new Values("v1", "v2")), 那么Tuple的属性即为：{ [ "v1" ], [ "V2" ] }
 * 
 * b. declarer.declare方法用来给我们发射的value在整个Stream中定义一个别名。可以理解为key。该值必须在整个topology定义中唯一。
 * 
 * 
 * 
 * Spout：
 * 在一个topology中产生源数据流的组件。
 * 通常情况下spout会从外部数据源中读取数据，然后转换为topology内部的源数据。
 * Spout是一个主动的角色，其接口中有个nextTuple()函数，storm框架会不停地调用此函数，用户只要在其中生成源数据即可。
 * 
 */
public class RandomSpout extends BaseRichSpout {

	private static final long serialVersionUID = 1L;

	private SpoutOutputCollector collector;

	private Random rand;

	private AtomicInteger count;

	private static String[] sentences = new String[] { "edi:I'm happy", "marry:I'm angry", "john:I'm sad", "ted:I'm excited", "laden:I'm dangerous" };

	/**
	 * open方法是初始化动作。允许你在该spout初始化时做一些动作，传入了上下文，方便取上下文的一些数据。
	 */
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		this.rand = new Random();
		this.count = new AtomicInteger();
	}

	@Override
	public void nextTuple() {
		try {
			String toSay = sentences[rand.nextInt(sentences.length)];
			int messageId = count.getAndDecrement();
			this.collector.emit(new Values(toSay), messageId);// messageId是ack需要的。Spout中，不指定messageId，使得Storm无法追踪；
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * Fields参数个数要与Values参数个数一致。
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentence"));
	}

	@Override
	public void ack(Object msgId) {
		System.err.println("ack " + msgId);
	}

	@Override
	public void fail(Object msgId) {
		System.err.println("fail " + msgId);
	}

	/**
	 * close方法在该spout关闭前执行，但是并不能得到保证其一定被执行。spout是作为task运行在worker内，在cluster模式下，supervisor会直接kill -9
	 * woker的进程，这样它就无法执行了。而在本地模式下，只要不是kill -9, 如果是发送停止命令，是可以保证close的执行的。
	 */
	@Override
	public void close() {
		System.out.println("close....");
	}
}