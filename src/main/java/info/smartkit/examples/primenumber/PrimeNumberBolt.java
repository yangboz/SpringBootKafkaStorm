package info.smartkit.examples.primenumber;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

/**
 * Created by smartkit on 2016/12/17.
 */
public class PrimeNumberBolt extends BaseRichBolt {
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = collector;
    }

    public void execute(Tuple tuple){
        int  number = tuple.getInteger(0);
        if(isPrime(number)){
            System.out.println( number );
        }
        collector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer){
        declarer.declare(new Fields("number"));
    }

    public boolean isPrime(int n){
        if( n == 1 || n == 2 || n == 3 )
        {
            return true;
        }

        // Is n an even number?
        if( n % 2 == 0 )
        {
            return false;
        }

        //if not, then just check the odds
        for( int i=3; i*i<=n; i+=2 )
        {
            if( n % i == 0)
            {
                return false;
            }
        }
        return true;
    }


}
