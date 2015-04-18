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
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class gridFilterBolt extends BaseBasicBolt {

	double curtstamp = TopologyMain.winSize - 1;

	int[] strid = new int[TopologyMain.nstream];
	int stridcnt = 0;
	int paircnt = 0;
	int[][] pairIdx = new int[TopologyMain.nstream][TopologyMain.nstream];

	public static int gfilter = 0;
	public int lfilter = 0;

	
	HashSet<String> hashPair = new HashSet<String>();
	int hashPairCnt=0;
	String hashStr=new String();
	


	// ................test.............................//

//	String[] qualPair = new String[1000];
//	int qualPairCnt = 0;
	
//	FileWriter fstream; // =new FileWriter("", true);
//	BufferedWriter out; // = new BufferedWriter(fstream);

	// ................................................//

	// public void tspairStrAna(String orgstr, int ts[]) {
	// int l = orgstr.length();
	// for (int i = 0; i < l; ++i) {
	// if (orgstr.charAt(i) == ',') {
	// ts[0] = Integer.valueOf(orgstr.substring(0, i));
	// ts[1] = Integer.valueOf(orgstr.substring(i + 1, l));
	// return;
	// }
	// }
	// return;
	// }
	public void tspairStrAna(String orgstr, int ts[]) {
		int l = orgstr.length();
		int pre = 0;
		int cnt = 0;
		for (int i = 0; i < l; ++i) {
			if (orgstr.charAt(i) == ',') {
				ts[cnt++] = Integer.valueOf(orgstr.substring(pre, i));
				pre = i + 1;

				if (cnt == 2)
					return;
				// ts[1] = Integer.valueOf(orgstr.substring(i + 1, l));
				// return;
			}
		}
		return;
	}

	public String tspairStrAna(String orgstr) {
		int l = orgstr.length();
		int pre = 0;
		int cnt = 0,i=0;
		for (i = 0; i < l; ++i) {
			if (orgstr.charAt(i) == ',') {
//				ts[cnt++] = Integer.valueOf(orgstr.substring(pre, i));
				cnt++;
				pre = i + 1;

				if (cnt == 2)
					break;
				// ts[1] = Integer.valueOf(orgstr.substring(i + 1, l));
				// return;
			}
		}
		return orgstr.substring(0, i);
//		return;
	}
	
	
	@Override
	public void cleanup() {
		// try {
		// out.close();
		// } catch (IOException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
		return;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {

		gfilter++;
		lfilter = gfilter;

//		try {
//			fstream = new FileWriter("gridRes.txt", false);
//			out = new BufferedWriter(fstream);
//			out.close();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		BufferedWriter out = new BufferedWriter(fstream);
		return;
	}

	public int localStreIdx(int hostid) {
		int i = 0;
		for (i = 0; i < stridcnt; ++i) {
			if (strid[i] == hostid) {
				break;
			}
		}
		if (i == stridcnt) {
			strid[stridcnt++] = hostid;
		}
		return i;

	}

	public int localPairStreIdx(String pairstr) {

		int streId[] = new int[4];
		tspairStrAna(pairstr, streId);

		int streNo1 = localStreIdx(streId[0]);
		int streNo2 = localStreIdx(streId[1]);
		if (pairIdx[streNo1][streNo2] == 0) {
			pairIdx[streNo1][streNo2] = 1;
			pairIdx[streNo2][streNo1] = 1;
			return 1;
		} else {
			return 0;
		}
	}

	public void localIdxRenew() {

		stridcnt = 0;
		int j = 0;
		for (int i = 0; i < TopologyMain.nstream; ++i) {
			for (j = 0; j < TopologyMain.nstream; ++j)
				pairIdx[i][j] = 0;
		}

		return;
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub

		double ts = input.getDoubleByField("ts");
		String pair = input.getStringByField("pair");

		int resnum = 0;

		if (ts > curtstamp) {

//			System.out.printf("time point  %f :  %d\n", ts - 1, qualPairCnt);
		
			
			//................result output....................................//
//			try {
//				fstream = new FileWriter("gridResTmp.txt", true);
//				BufferedWriter out = new BufferedWriter(fstream);
//
//				out.write("Timestamp  " + Double.toString(curtstamp) + ", "
//						+ "total num  " + Integer.toString(qualPairCnt) + ":  ");
//
//				for (int i = 0; i < qualPairCnt; ++i) {
//					out.write(qualPair[i] + "   ");
//				}
//				out.write("\n");
//				out.close();
//			} catch (IOException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
			//................................................................//

			// ....test.........//
//			qualPairCnt = 0;
			//.................//
			
				
			//..............hash method..................//
			hashPairCnt=0;
			hashPair.clear();
			
			hashStr=tspairStrAna(pair); 
			hashPairCnt++;
			hashPair.add(hashStr);
			//...........................................//
			
			//...............matrix method................//
//			paircnt = 0;
//			localIdxRenew();
//			if (localPairStreIdx(pair) == 1) {
//				paircnt++;
//
//				// ....test.........//
////				qualPair[qualPairCnt++] = pair;
//				//.................//
//			}
			//...........................................//

			curtstamp = ts;

		} else if (ts < curtstamp) {
			System.out.printf("!!!!!!!!!!!!! time sequence disorder\n");
		} else if (Math.abs(ts - curtstamp) <= 1e-3) {

			
			//.............hash method.................//
			
			hashStr=tspairStrAna(pair); 	
			if(hashPair.contains(hashStr)==false)
			{
			   hashPairCnt++;
			   hashPair.add(hashStr);
			}
				
			//........................................//
			
			//.............matrix method.............//
//			if (localPairStreIdx(pair) == 1) {
//				paircnt++;
//
//				// ....test.........//
////				qualPair[qualPairCnt++] = pair;
//				//...................//
//
//			}
			//.........................................//
			
		}
		return;
	}
}
