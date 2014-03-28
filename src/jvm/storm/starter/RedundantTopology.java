package storm.starter;

public class RedundantTopology {

  public static class ProfileGenerator extends BaseRichSpout {
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    }

    @Override
    public void nextTuple() {
    }

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
  }

  public static class MaleFilter extends BaseRichBolt {
    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
    }

    @Override
    public void execute(Tuple tuple) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
  }

  public static class YouthFilter extends BaseRichBolt {
    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
    }

    @Override
    public void execute(Tuple tuple) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
  }

  public static class ElderFilter extends BaseRichBolt {
    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
    }

    @Override
    public void execute(Tuple tuple) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
  }



  public static class PrinterBolt extends BaseBasicBolt {
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      System.out.println(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
    }

  public static void main(String[] args) throws Exception {
    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("people", new ProfileGenerator(), 1);
    builder.setBolt("males1", new MaleFilterBolt(), 1).shuffleGrouping("people");
    builder.setBolt("males2", new MaleFilterBolt(), 1).shuffleGrouping("people");
    builder.setBolt("youth-males", new ExclamationBolt(), 1).shuffleGrouping("male1");
    builder.setBolt("elder-males", new ExclamationBolt(), 1).shuffleGrouping("male2");

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