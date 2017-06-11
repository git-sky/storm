package cn.com.sky.storm.ack.rich;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

/**
 * <pre>
 * 保证消息可靠性需要四个方面的设置：
 * 1.acker线程数目>0，才会真正的追踪消息;如果acker线程=0，则一发送就ack，并不保证消息处理成功。
 * 2.spout发射带有messageId,才会追踪消息;否则不会有ack或者fail。
 * 3.bolt设置锚定，才会追踪下一级消息;否则不关心下一级消息的成功与失败。
 * 4.bolt设置ack，才能ack成功，否则会fail。
 * </pre>
 */
public class ExclaimBasicTopo {

	public static void main(String[] args) throws Exception {

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("spout", new RandomRichSpout());
		builder.setBolt("exclaim", new ExclaimRichBolt()).shuffleGrouping("spout");
		builder.setBolt("print", new PrintRichBolt(), 3).shuffleGrouping("exclaim");

		// builder.setBolt("exclaim", new ExclaimBasicBolt(), 2).shuffleGrouping("spout");
		// builder.setBolt("print", new PrintBolt(),3).shuffleGrouping("exclaim");

		Config conf = new Config();
		conf.setDebug(false);
		conf.setMessageTimeoutSecs(6);//超时时间
//		conf.setNumAckers(0);// acker线程数量,0 的时候不真正追踪消息，自动ack.
		 conf.setMaxSpoutPending(3);//spout限流,超过3个消息没有ack或者fail，不发送消息。

		if (args != null && args.length > 0) {
			conf.setNumWorkers(3);// work进程数量

			StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
		} else {

			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("test", conf, builder.createTopology());
			Utils.sleep(100000);
			cluster.killTopology("test");
			cluster.shutdown();
		}
	}
}