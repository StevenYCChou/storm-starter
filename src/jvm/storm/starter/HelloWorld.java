package storm.starter;


import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.tuple.Fields;
import backtype.storm.task.TopologyContext;
import backtype.storm.utils.Utils;

import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;

import java.util.Map;


public class HelloWorld {

  public static class HelloWorldSpout extends BaseRichSpout {
    SpoutOutputCollector _collector;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
      _collector = collector;
    }

    @Override
    public void nextTuple() {
      Utils.sleep(100);
      String sentence = "Hello World";
      _collector.emit(new Values(sentence));
    }

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word"));
    }
  }

  public static class TestPrinterBolt extends BaseBasicBolt {
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

    //Set up the topology
    builder.setSpout("hello-world", new HelloWorldSpout(), 10); //the last parameter is specify the number of workers
    builder.setBolt("print-hellow-world", new TestPrinterBolt(), 3).shuffleGrouping("hello-world");

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