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

  public static class SexFilterBolt extends BaseRichBolt {
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

  public static class YouthFilterBolt extends BaseRichBolt {
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

  public static class ElderFilterBolt extends BaseRichBolt {
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
  }
}