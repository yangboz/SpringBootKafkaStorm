package info.smartkit.examples.wordcount;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;

/**
 * Created by smartkit on 2016/12/18.
 */
public class SentenceSpout extends BaseRichSpout{

    private static final long serialVersionUID = 3444934973982660864L;
    private SpoutOutputCollector collector;
    private String[] sentences = { "my dog has fleas", "i like cold beverages",
                         "the dog ate my homework", "don't have a cow man",
                         "i don't think i like fleas" };

    private int index = 1;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("sentence"));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        this.collector.emit(new Values(sentences[index]));
        index++;
        if(index>=sentences.length){
            index  = 0;
        }
        Utils.sleep(100);
    }
}
