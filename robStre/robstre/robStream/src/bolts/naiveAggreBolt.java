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

	double curtstamp = TopologyMain.winSize - 1;

	FileWriter fstream; // =new FileWriter("", true);
	BufferedWriter out; // = new BufferedWriter(fstream);

	HashSet<String> pairHset = new HashSet<String>();

	// ................................................//

	// public int localStreIdx(int streid) {
	// int i = 0;
	// for (i = 0; i < stridcnt; ++i) {
	// if (strid[i] == streid) {
	// break;
	// }
	// }
	// if (i == stridcnt) {
	// strid[stridcnt++] = streid;
	// }
	// return i;
	// }

	// public void pairAna(String pair,int pairInt[])
	// {
	// int len=pair.length();
	// for(int i=0;i<len;++i)
	// {
	// if(pair.charAt(i)==',')
	// {
	// pairInt[0]=Integer.valueOf(pair.substring(0, i));
	// pairInt[1]=Integer.valueOf(pair.substring(i+1, len));
	// return;
	// }
	// }
	// return;
	// }

	// public void pairAna(String pair,int pairInt[])
	// {
	// int len=pair.length();
	// int cnt=0,pre=0;
	//
	// // System.out.printf("%s\n",pair);
	//
	// for(int i=0;i<len;++i)
	// {
	// if(pair.charAt(i)==',')
	// {
	// pairInt[cnt++]=Integer.valueOf(pair.substring(pre, i));
	// pre=i+1;
	//
	// if(cnt==2)
	// return;
	//
	// // pairInt[1]=Integer.valueOf(pair.substring(i+1, len));
	// // return;
	// }
	// }
	// return;
	// }

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

			try {
				 fstream = new FileWriter("naiveRes.txt", true);
				BufferedWriter out = new BufferedWriter(fstream);

				out.write("Timestamp  " + Double.toString(curtstamp) + ", "
						+ "total num  " + Integer.toString(pairHset.size())
						+ ": \n ");

				int len = pairHset.size();

				for (String iter : pairHset) {
					out.write(iter + "\n");
				}
				
				out.write("\n");
				out.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

//			System.out.printf("time stamp %f:   %d\n ", curtstamp, rescnt);
			

			//..........update...........//
			
			pairHset.clear();
			pairHset.add(pair);


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
