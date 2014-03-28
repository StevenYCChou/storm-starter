package storm.starter;


import backtype.storm.task.TopologyContext;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;

import backtype.storm.utils.Utils;
import java.util.Map;
import java.util.Random;

public class RedundantlessTopology {

  public static class ProfileGenerator extends BaseRichSpout {
    SpoutOutputCollector _collector;
    Random _rand;
    Integer _id;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
      _collector = collector;
      _rand = new Random();
      _id = 0;
    }

    @Override
    public void nextTuple() {
      Utils.sleep(50);

      String[] genders = new String[]{ "Male", "Female" };
      String gender = genders[_rand.nextInt(genders.length)];

      Integer[] ages = new Integer[]{ 10, 15, 20, 25, 30,
                                      35, 40, 45, 50, 55,
                                      60, 65, 70, 75, 80,
                                      85};
      Integer age = ages[_rand.nextInt(ages.length)];

      _collector.emit(new Values(_id, gender, age));
      _id += 1;
    }

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("id", "gender", "age"));
    }
  }

  public static class MaleFilter extends BaseRichBolt {
    OutputCollector _collector;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
      _collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
      if(tuple.getString(1).equals("Male")){
        _collector.emit(tuple, new Values(tuple.getInteger(0), tuple.getString(1), tuple.getInteger(2)));
        _collector.ack(tuple);
      }
    }

    @Override
    public void cleanup(){
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("id", "gender", "age"));
    }
  }

  public static class YouthFilter extends BaseRichBolt {
    OutputCollector _collector;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
      _collector = collector;    
    }

    @Override
    public void execute(Tuple tuple) {
      if(tuple.getInteger(2) <= 30){
        _collector.emit(tuple, new Values(tuple.getInteger(0), tuple.getString(1), tuple.getInteger(2)));
        _collector.ack(tuple);
      }
    }

    @Override
    public void cleanup(){
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("id", "gender", "age"));
    }
  }

  public static class ElderFilter extends BaseRichBolt {
    OutputCollector _collector;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
      _collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
      if(tuple.getInteger(2) >= 65){
        _collector.emit(tuple, new Values(tuple.getInteger(0), tuple.getString(1), tuple.getInteger(2)));
        _collector.ack(tuple);
      }
    }

    @Override
    public void cleanup(){
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("id", "gender", "age"));
    }
  }

  public static class Printer extends BaseBasicBolt {
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      System.out.println(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
    }
  }

  public static void main(String[] args) throws Exception {
    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("people", new ProfileGenerator(), 1);
    builder.setBolt("males", new MaleFilter(), 1).shuffleGrouping("people");
    builder.setBolt("youth-males", new YouthFilter(), 1).shuffleGrouping("males");
    builder.setBolt("elder-males", new ElderFilter(), 1).shuffleGrouping("males");
    builder.setBolt("youth-males-printer", new Printer(), 1).shuffleGrouping("youth-males");
    builder.setBolt("elder-males-printer", new Printer(), 1).shuffleGrouping("elder-males");

    Config conf = new Config();
    conf.setDebug(true);

    if (args != null && args.length > 0) {
      conf.setNumWorkers(3);

      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }
    else {

      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("test", conf, builder.createTopology());
      Utils.sleep(10000);
      cluster.killTopology("test");
      cluster.shutdown();
    }

  }
}