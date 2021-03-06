package bolts;

import java.util.HashSet;
import java.util.Map;

import main.TopologyMain;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class rpAggreBolt extends BaseBasicBolt {

	// .............aggregator....................//

	HashSet<String> strePair = new HashSet<String>();
	// ............input time order..............//

	double ts = 0.0;

	public double curtstamp = TopologyMain.winSize - 1;
	public double ststamp = 0.0;

	/**
	 * At the end of the spout (when the cluster is shutdown We will show the
	 * word counters
	 */
	@Override
	public void cleanup() {

	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		// TODO Auto-generated method stub

		return;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

		return;
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub

		double ts = input.getDoubleByField("ts");
		String pairstr = input.getStringByField("pair");

		if (ts > curtstamp) {

			// .............test................			
			
//			System.out.printf("AggreBolt has %d resutls at %f \n", strePair.size(),curtstamp);
			
			// .................................

			// .........update for the next sliding window..........//

			curtstamp = ts;
			strePair.clear();

			strePair.add(pairstr);

		} else if (ts < curtstamp) {
			System.out
					.printf("!!!!!!!!!!!!! direct AggreBolt time sequence disorder\n");
		} else if (Math.abs(ts - curtstamp) <= 1e-3) {

			strePair.add(pairstr);
		}

		return;

	}

}
