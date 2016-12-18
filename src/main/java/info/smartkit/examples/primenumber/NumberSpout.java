package info.smartkit.examples.primenumber;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;


/**
 * Created by smartkit on 2016/12/16.
 */
public class NumberSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private static int currentNumber = 1;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector){
        this.collector = collector;
    }

    @Override
    public void nextTuple(){
        //Emit next collector.
        collector.emit(new Values(new Integer(currentNumber++)));
    }

    @Override
    public void ack(Object id){

    }

    @Override
    public void fail(Object id){

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("number"));
    }
}
