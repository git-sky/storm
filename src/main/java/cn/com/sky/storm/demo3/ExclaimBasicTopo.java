package cn.com.sky.storm.demo3;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public class ExclaimBasicTopo {

	public static void main(String[] args) throws Exception {

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("spout", new RandomSpout());
		builder.setBolt("exclaim", new ExclaimRichBolt()).shuffleGrouping("spout");
		builder.setBolt("print", new PrintBolt(), 3).shuffleGrouping("exclaim");

		// builder.setBolt("exclaim", new ExclaimBasicBolt(), 2).shuffleGrouping("spout");
		// builder.setBolt("print", new PrintBolt(),3).shuffleGrouping("exclaim");

		Config conf = new Config();
		conf.setDebug(false);

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