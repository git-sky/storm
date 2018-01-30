package cn.com.sky.storm.demo2;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public class ExclaimBasicTopology {

	public static void main(String[] args) throws Exception {

		TopologyBuilder builder = new TopologyBuilder();

//        一个task就是一个处理逻辑的实例。
		// parallelism来规定executor的数量
		// 下面会有两个exectuor去执行两个exclaimBasicBolt的task，3个executor去执行3个PrintBolt的task。
		builder.setSpout("spout", new RandomSpout());// 定义一个spout，id为"spout"
		builder.setBolt("exclaim", new ExclaimBasicBolt(), 2).shuffleGrouping("spout");// 定义了一个id为"exclaim"的bolt，并且按照随机分组获得"spout"发射的tuple
		builder.setBolt("print", new PrintBolt(), 3).shuffleGrouping("exclaim");// 定义了一个id为"print"的bolt，并且按照随机分组获得"exclaim”发射出来的tuple

		Config conf = new Config();
		conf.setDebug(false);

		try {

			if (args != null && args.length > 0) {
				conf.setNumWorkers(3);
				StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
			} else {
				LocalCluster cluster = new LocalCluster();
				cluster.submitTopology("test", conf, builder.createTopology());
				Utils.sleep(5000);
				cluster.killTopology("test");
				cluster.shutdown();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
