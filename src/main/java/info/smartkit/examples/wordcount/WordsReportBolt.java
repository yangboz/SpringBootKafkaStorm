package info.smartkit.examples.wordcount;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.*;

/**
 * Created by smartkit on 2016/12/18.
 */
public class WordsReportBolt extends BaseRichBolt{

    private static final long serialVersionUID = 4921144902730095910L;
    private HashMap<String,Long> counts = null;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.counts = new HashMap<String, Long>();
    }

    @Override
    public void execute(Tuple tuple) {
        String word = tuple.getStringByField("word");
        Long count =  tuple.getLongByField("count");
        this.counts.put(word,count);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    public void clearup(){
        System.out.println("-----print count-----");
        List<String> keys = new ArrayList<String>();
        keys.addAll(counts.keySet());
        Collections.sort(keys);
        for (String key: keys){
            System.out.println("key: "+counts.get(key));
        }
        System.out.println("-----end of print count---");
    }
}
