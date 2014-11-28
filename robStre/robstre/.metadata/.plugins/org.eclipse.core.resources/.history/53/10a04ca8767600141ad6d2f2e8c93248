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

public class naiveAggreBolt extends BaseBasicBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	double curtstamp = TopologyMain.winSize - 1;

	FileWriter fstream; // =new FileWriter("", true);
	BufferedWriter out; // = new BufferedWriter(fstream);

	HashSet<String> pairHset = new HashSet<String>();

	@Override
	public void cleanup() {

		return;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		try {
			fstream = new FileWriter("naiveRes.txt", false);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return;
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub

		double ts = input.getDoubleByField("ts");
		String pair = input.getStringByField("pair");

		if (ts > curtstamp) {

			
			// ...........test........
//			try {
//
//				fstream = new FileWriter("naiveRes.txt", true);
//				BufferedWriter out = new BufferedWriter(fstream);
//
//				out.write("Timestamp  " + Double.toString(curtstamp) + ", "
//						+ "total num  " + Integer.toString(pairHset.size())
//						+ ": \n ");
//
//				int len = pairHset.size();
//
//				for (String iter : pairHset) {
//					out.write(iter + "\n");
//			}
//
				System.out.printf("timestamp %f  %d\n", curtstamp, pairHset.size());
//
//				out.write("\n");
//				out.close();
//				
//
//			} catch (IOException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}	
			// ..................

			// ..........update...........//

			pairHset.clear();
			pairHset.add(pair);
			curtstamp = ts;

		} else if (ts < curtstamp) {
			System.out
					.printf("!!!!!!!!!!!!! naive aggregate bolt: time sequence disorder\n");
		} else if (Math.abs(ts - curtstamp) <= 1e-3) {

			pairHset.add(pair);
		}
		return;

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
	}
}
