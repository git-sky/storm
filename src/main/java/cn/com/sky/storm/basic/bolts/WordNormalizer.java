package cn.com.sky.storm.basic.bolts;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class WordNormalizer extends BaseBasicBolt {

	private static final long serialVersionUID = 1L;

	public void cleanup() {
	}

	/**
	 * The bolt will receive the line from the words file and process it to Normalize this line
	 * 
	 * The normalize will be put the words in lower case and split the line to get all words in this
	 */
	public void execute(Tuple input, BasicOutputCollector collector) {
		System.out.println(input.getFloatByField("line"));
		String sentence = input.getString(0);
		String[] words = sentence.split(" ");
		for (String word : words) {
			word = word.trim();
			if (!word.isEmpty()) {
				word = word.toLowerCase();
				System.out.println("word:" + word);
				collector.emit(new Values(word));
			}
		}
	}

	/**
	 * The bolt will only emit the field "word"
	 */
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}
}
