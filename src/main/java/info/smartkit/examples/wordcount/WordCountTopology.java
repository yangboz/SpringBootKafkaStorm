package info.smartkit.examples.wordcount;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

/**
 * Created by smartkit on 2016/12/18.
 */
public class WordCountTopology {

    private static final String SPOUT_ID = "sentence-spout";
    private static final String BOLT_ID_SENTENCE_SPLIT = "sentence-split-bolt";
    private static final String BOLT_ID_WORD_COUNT = "word-count-bolt";
    private static final String BOLT_ID_COUNT_REPORT = "count--report-bolt";
    private static final String TOPOLOGY_ID = "word-count-topology";


    public static void main(String[] args){
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout(SPOUT_ID,new SentenceSpout());
        topologyBuilder.setBolt(BOLT_ID_SENTENCE_SPLIT,new SentenceSplitBolt()).shuffleGrouping(SPOUT_ID);
        topologyBuilder.setBolt(BOLT_ID_WORD_COUNT,new WordCountBlot()).fieldsGrouping(BOLT_ID_SENTENCE_SPLIT,new Fields("word"));
        topologyBuilder.setBolt(BOLT_ID_COUNT_REPORT,new WordsReportBolt()).globalGrouping(BOLT_ID_WORD_COUNT);

        Config config  = new Config();
        LocalCluster localCluster = new LocalCluster();

        localCluster.submitTopology(TOPOLOGY_ID,config,topologyBuilder.createTopology());
        //
        Utils.sleep(10000);
        localCluster.killTopology(TOPOLOGY_ID);
        localCluster.shutdown();
    }
}
