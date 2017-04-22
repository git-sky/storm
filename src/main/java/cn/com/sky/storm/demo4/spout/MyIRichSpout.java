package cn.com.sky.storm.demo4.spout;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * <pre>
 * 
 * open()
 * 方法是初始化动作。允许你在该spout初始化时做一些动作，传入了上下文，方便取上下文的一些数据。
 * 
 * close()
 * 方法在该spout关闭前执行，但是并不能得到保证其一定被执行。
 * spout是作为task运行在worker内，在cluster模式下，supervisor会直接kill -9 woker的进程，这样它就无法执行了。
 * 而在本地模式下，只要不是kill -9, 如果是发送停止命令，是可以保证close的执行的。
 * 
 * activate()和deactivate()
 * 一个spout可以被暂时激活和关闭，这两个方法分别在对应的时刻被调用。
 * 
 * nextTuple()
 * 用来发射数据。
 * 
 * ack(Object) 
 * 传入的Object其实是一个id，唯一表示一个tuple。该方法是这个id所对应的tuple被成功处理后执行。
 * 
 * fail(Object)
 * 同ack，只不过是tuple处理失败时执行。
 * 
 * 
 * 
 * 通常情况下（Shell和事务型的除外），实现一个Spout，可以直接实现接口IRichSpout，如果不想写多余的代码，可以直接继承BaseRichSpout。
 * 
 * 
 * ack，fail 和 nextTuple是在同一个线程中完成的。
 * 
 * 
 * Storm是否会自动重发失败的Tuple？
 * 这里答案已经很明显了。fail方法如何实现取决于你自己。只有在fail中做了重发机制，才有重发。
 * 注：Trident除外。这是Storm提供的特殊的事务性API，它确实会帮你自动重发的。
 * 
 * </pre>
 */
public class MyIRichSpout implements IRichSpout {

	private static final long serialVersionUID = 1L;

	private SpoutOutputCollector collector;

	private List<Object> list = new ArrayList<Object>();
	private Random random = new Random();
	private AtomicLong counter = new AtomicLong();

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		for (int i = 0; i < 10; i++) {
			list.add("element=" + i);
		}
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

	@Override
	public void activate() {
		// TODO Auto-generated method stub

	}

	@Override
	public void deactivate() {
		// TODO Auto-generated method stub

	}

	@Override
	public void nextTuple() {

		try {
			if (list.size() == 0) {
				// System.out.println("over");
				return;

			}
			int index = random.nextInt(list.size());

			Object obj = list.get(index);
			list.remove(index);

			long messageId = counter.getAndIncrement();
			this.collector.emit(new Values("a"), messageId);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * ack(Object msgId)方法的msgId就是emit的messageId。
	 * 
	 * ack对于每个tuple只调用了一次
	 */
	@Override
	public void ack(Object msgId) {
		System.err.println("ack:" + msgId);
	}

	/**
	 * fail(Object msgId)方法的msgId就是emit的messageId。
	 */
	@Override
	public void fail(Object msgId) {
		System.err.println("fail:" + msgId);

	}

	/**
	 * declarer.declare方法用来给我们发射的value在整个Stream中定义一个别名。可以理解为key。该值必须在整个topology定义中唯一。
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentence"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
