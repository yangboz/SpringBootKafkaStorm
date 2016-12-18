package info.smartkit.examples.wordcount;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by smartkit on 2016/12/18.
 */
public class WordCountBlot extends BaseRichBolt{

    private OutputCollector collector;
    private HashMap<String,Long> counts = null;


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.counts = new HashMap<String, Long>();
    }

    @Override
    public void execute(Tuple tuple) {

        String word = tuple.getStringByField("word");
        Long count = this.counts.get(word);
        if(count==null)
        {
            count = 0L;
        }
        count++;
        this.counts.put(word,count);
        this.collector.emit(new Values(word,counts));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word","count"));

    }
}
