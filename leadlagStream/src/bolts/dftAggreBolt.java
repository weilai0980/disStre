package bolts;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;

import main.TopologyMain;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class dftAggreBolt extends BaseBasicBolt {

	// .............aggregator....................//

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	HashSet<String> strePair = new HashSet<String>();
	// ............input time order..............//

	double ts = 0.0;
	double curtstamp = TopologyMain.winSize - 1;

	FileWriter fstream; // =new FileWriter("", true);
	BufferedWriter out; // = new BufferedWriter(fstream);

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

		try {
			fstream = new FileWriter("dftRes.txt", false);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

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

			// ...........test........

			System.out.printf("AggreBolt at timestamp %f has %d\n", curtstamp,
					strePair.size());

			// try {
			//
			// fstream = new FileWriter("dftRes.txt", true);
			// BufferedWriter out = new BufferedWriter(fstream);
			//
			// out.write("Timestamp  " + Double.toString(curtstamp) + ", "
			// + "total num  " + Integer.toString(strePair.size())
			// + ": \n ");
			//
			// for (String iter : strePair) {
			// out.write(iter + "\n");
			// }
			//
			// out.write("\n");
			// out.close();
			//
			// } catch (IOException e) {
			// // TODO Auto-generated catch block
			// e.printStackTrace();
			// }
			// ..................

			// .........update for the next sliding window..........//

			curtstamp = ts;
			strePair.clear();
			strePair.add(pairstr);

		} else if (ts < curtstamp) {
			System.out
					.printf("!!!!!!!!!!!!! dftAggreBolt time sequence disorder\n");
		} else if (Math.abs(ts - curtstamp) <= 1e-3) {

			strePair.add(pairstr);

		}
		return;
	}
}
