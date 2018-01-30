package cn.com.sky.storm.ack.rich;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class RandomRichSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;

    private Random rand;

    private AtomicInteger counter;

    private static String[] sentences = new String[]{"edi:I'm happy", "marry:I'm angry", "john:I'm sad", "ted:I'm excited", "laden:I'm dangerous"};

    /**
     * storm会执行spout的open和各个bolt的prepare，依次都初始化好后会激活spout（Activate spout），这时spout工作线程才开始执行nextTuple。
     *
     * @param conf
     * @param context
     * @param collector
     */
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.rand = new Random();
        this.counter = new AtomicInteger();
    }

    /**
     * ack，fail 和 nextTuple是在同一个线程中完成的。
     */
    @Override
    public void nextTuple() {
//		Utils.sleep(1000);
        String toSay = sentences[rand.nextInt(sentences.length)];
        int msgId = counter.getAndIncrement();
        toSay = "[" + msgId + "] " + toSay;
        PrintHelper.print("Send " + toSay);

        //2.spout发射带有messageId,才会追踪消息;否则不会有ack或者fail。
        this.collector.emit(new Values(toSay), msgId);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sentence"));
    }

    @Override
    public void ack(Object msgId) {
        System.err.println("ack: " + msgId);
    }

    @Override
    public void fail(Object msgId) {
        System.err.println("fail: " + msgId);
    }

}