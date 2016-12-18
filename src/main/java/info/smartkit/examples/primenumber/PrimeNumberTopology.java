package info.smartkit.examples.primenumber;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

/**
 * Created by smartkit on 2016/12/17.
 */
public class PrimeNumberTopology {

    public static void main(String[] args){
    //
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout",new NumberSpout());
        builder.setBolt("bolt",new PrimeNumberBolt()).shuffleGrouping("spout");
//
        Config config = new Config();
        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("test",config,builder.createTopology());
        Utils.sleep(10000);
        localCluster.killTopology("test");
        localCluster.shutdown();
    }

}
