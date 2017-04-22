package cn.com.sky.storm.demo4;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import cn.com.sky.storm.demo4.bolt.MyBaseBasicBolt;
import cn.com.sky.storm.demo4.bolt.MyIRichBolt;
import cn.com.sky.storm.demo4.spout.MyIRichSpout;

public class MyTopology {

	public static void main(String[] args) {

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("mySpout", new MyIRichSpout());
		builder.setBolt("myBolt", new MyIRichBolt()).shuffleGrouping("mySpout");

		LocalCluster cluster = new LocalCluster();

		String str = "test";

		Config conf = new Config();
		conf.setDebug(false);

		// 超时时间
		conf.setMessageTimeoutSecs(10);

		StormTopology topology = builder.createTopology();

		try {
			cluster.submitTopology(str, conf, topology);
		} catch (Exception e) {
			System.out.println("exception");
			e.printStackTrace();
		} catch (Throwable t) {
			t.printStackTrace();
		}

		Utils.sleep(50000);
		cluster.killTopology(str);
		cluster.shutdown();

	}

}
