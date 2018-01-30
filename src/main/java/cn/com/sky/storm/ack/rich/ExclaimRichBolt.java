package cn.com.sky.storm.ack.rich;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * 通常情况下，实现一个Bolt，可以实现IRichBolt接口或继承BaseRichBolt，如果不想自己处理结果反馈，可以实现IBasicBolt接口或继承BaseBasicBolt，
 * 它实际上相当于自动做掉了prepare方法和collector.emit.ack(inputTuple)；
 */
public class ExclaimRichBolt extends BaseRichBolt {

    private static final long serialVersionUID = 1L;

    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {

        System.out.println("===================begin====================");
        System.out.println(tuple.getStringByField("sentence"));
        System.out.println(tuple.getValueByField("sentence"));
        System.out.println(tuple.getFields());
        System.out.println("====================end===================");

        //3.bolt设置锚定，才会追踪下一级消息;否则不关心下一级消息的成功与失败。
        // 第一个参数是传入Bolt的tuple，第二个参数是新产生的tuple的value，这种emit的方式，在Storm中称为: "anchor"。
        this.collector.emit(tuple, new Values(tuple.getString(0) + "!"));


//        4.bolt设置ack，才能ack成功，否则会fail。
        // boltTask在创建子tuple时并不会向acker发送消息让其跟踪，而是很巧妙的省略了这一步：
        // bolt在发射一个新的bolt的时候会把这个新tuple跟它的父tuple的关系保存起来(strom称之为anchoring)。然后在ack
        // tuple的时候，storm会把要ack的tuple的id, 以及
        // 这个tuple新创建的所有的tuple的id的异或值发送给acker。消息格式是:(spout-tuple-id,tmp-ack-val)执行完这一步后，ack-val的值就变成了所有子tuple的id的异或值
        this.collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("after_excl"));
    }

}