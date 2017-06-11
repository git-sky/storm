package cn.com.sky.storm.basic;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import cn.com.sky.storm.basic.bolts.WordCounter;
import cn.com.sky.storm.basic.bolts.WordNormalizer;
import cn.com.sky.storm.basic.spouts.WordReader;

public class TopologyMain {
	public static void main(String[] args) throws InterruptedException {

		// Topology definition
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("word-reader", new WordReader());
		builder.setBolt("word-normalizer", new WordNormalizer()).shuffleGrouping("word-reader");
		builder.setBolt("word-counter", new WordCounter(), 1).setNumTasks(3).fieldsGrouping("word-normalizer", new Fields("word"));

		// Configuration
		Config conf = new Config();
		conf.put("wordsFile", "e:/words.txt");
		conf.setDebug(false);

		// Topology run
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		LocalCluster cluster = new LocalCluster();

		try {
			cluster.submitTopology("Getting-Started-Toplogie", conf, builder.createTopology());
		} catch (Exception e) {
			e.printStackTrace();
		}
		Thread.sleep(1000);
		cluster.shutdown();
	}
}
