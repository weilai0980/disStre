package bolts;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import main.TopologyMain;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class robAggreBolt extends BaseBasicBolt {

	// .............aggregator....................//

	HashSet<String> strePair = new HashSet<String>();
	// ............input time order..............//

	double ts = 0.0;

	public double curtstamp = TopologyMain.winSize - 1;
	public double ststamp = 0.0;

	// ........test output.............//
	FileWriter fstream; // =new FileWriter("", true);
	BufferedWriter out; // = new BufferedWriter(fstream);

	@Override
	public void cleanup() {

	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		// TODO Auto-generated method stub

		try {
			fstream = new FileWriter("robRes.txt", false);
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
			try {

				fstream = new FileWriter("robRes.txt", true);
				BufferedWriter out = new BufferedWriter(fstream);

				out.write("Timestamp  " + Double.toString(curtstamp) + ", "
						+ "total num:  " + Integer.toString(strePair.size())
						+ ": \n ");

				int len = strePair.size();

				for (String iter : strePair) {
					out.write(iter + "\n");
				}
//
				System.out.printf("Aggrebolt timestamp %f:   %d \n", curtstamp, strePair.size());
				
//				for(String str:strePair)
//				{
//					System.out.printf(" %s", str);
//				}
//				System.out.printf(" \n");
//
				out.write("\n");
				out.close();

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			// ..................

			// .........update for the next sliding window..........//

			curtstamp = ts;
			strePair.clear();

			strePair.add(pairstr);

		} else if (ts < curtstamp) {
			System.out
					.printf("!!!!!!!!!!!!! direct robAggreBolt time sequence disorder\n");
		} else if (Math.abs(ts - curtstamp) <= 1e-3) {

			strePair.add(pairstr);
		}

		return;

	}
}
