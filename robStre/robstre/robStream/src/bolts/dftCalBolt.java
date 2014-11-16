package bolts;

import main.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class dftCalBolt extends BaseBasicBolt {

	// ............input time order..............//

	double curtstamp = TopologyMain.winSize - 1;
	String streType = new String();
	double ts = 0.0;

	String commandStr = new String();
	long preTaskId = 0;

	HashSet<Long> preTaskIdx = new HashSet<Long>();
	// .........memory management for sliding windows.................//

	double[][] streamVec = new double[TopologyMain.nstreBolt + 10][TopologyMain.winSize + 10];
	double[][] dftVec = new double[TopologyMain.nstreBolt + 10][TopologyMain.winSize + 10];
	int[][] cellVec = new int[TopologyMain.gridIdxN + 5][TopologyMain.winSize + 5];

	HashMap<Integer, Integer> streIdx = new HashMap<Integer, Integer>();
	HashMap<String, Integer> cellIdx = new HashMap<String, Integer>();

	List<List<Integer>> cellHostStre = new ArrayList<List<Integer>>(
			TopologyMain.gridIdxN + 5);
	List<List<Integer>> cellNbStre = new ArrayList<List<Integer>>(
			TopologyMain.gridIdxN + 5);

	ArrayList<String> resPair = new ArrayList<String>();

	// ...........computation parameter....................//
	int locTaskId;
	double disThre = 2 - 2 * TopologyMain.thre;
	double cellEps = Math.sqrt(disThre);
	
	int recStreCnt=0;
	
	

	public int vecAna(double streamVec[][], int idx, String vecstr) {

		int len = vecstr.length();
		int cnt = 0, pre = 0;
		for (int i = 0; i < len; ++i) {
			if (vecstr.charAt(i) == ',') {
				streamVec[idx][cnt++] = Double
						.valueOf(vecstr.substring(pre, i));
				pre = i + 1;
			}
		}
		return cnt;
	}

	public void vecAnaInt(int mem[][], int idx, String vecstr) {
		int len = vecstr.length(), pre = 0, cnt = 0, tmpval = 0;

		for (int i = 0; i < len; ++i) {
			if (vecstr.charAt(i) == ',') {

				mem[idx][cnt++] = Integer.valueOf(vecstr.substring(pre, i));
				pre = i + 1;

			}
		}

		return;
	}

	public void IndexingStre(int streid, String streamStr, String dftStr,
			String cellStr, int flag) {
		//

		int streNo = 0;
		if (streIdx.containsKey(streid) == true) {

			streNo = streIdx.get(streid);

		} else {
			streNo = streIdx.size();
			streIdx.put(streid, streNo);
		}

		int cellNo = cellIdx.size();
		// streIdx.put(streid, streNo);

		if (cellIdx.containsKey(cellStr) == true) {

			if (flag == 1) {
				cellHostStre.get(cellIdx.get(cellStr)).add(streid);
			} else {
				cellNbStre.get(cellIdx.get(cellStr)).add(streid);
			}
		} else {

			cellIdx.put(cellStr, cellNo);
			cellHostStre.add(new ArrayList<Integer>());
			cellNbStre.add(new ArrayList<Integer>());

			if (flag == 1) {
				cellHostStre.get(cellIdx.get(cellStr)).add(streid);
			} else {
				cellNbStre.get(cellIdx.get(cellStr)).add(streid);
			}
		}

		vecAna(streamVec, streNo, streamStr);
		vecAna(dftVec, streNo, dftStr);
		vecAnaInt(cellVec, cellNo, cellStr);

		return;
	}

	int correCalDisReal(double thre, int streid1, int streid2) {

		int memidx1 = streIdx.get(streid1), memidx2 = streIdx.get(streid2), k = 0;

		double tmpres = 0.0;

		for (k = 0; k < TopologyMain.winSize; ++k) {
			tmpres = tmpres + (streamVec[memidx1][k] - streamVec[memidx2][k])
					* (streamVec[memidx1][k] - streamVec[memidx2][k]);
		}

		return tmpres <= thre ? 1 : 0;

	}

	int correCalDisDFT(double thre, int streid1, int streid2) {

		int memidx1 = streIdx.get(streid1), memidx2 = streIdx.get(streid2), k = 0;

		double tmpres = 0.0;

		for (k = 0; k < TopologyMain.dftN * 2; ++k) {
			tmpres = tmpres + (dftVec[memidx1][k] - dftVec[memidx2][k])
					* (dftVec[memidx1][k] - dftVec[memidx2][k]);
		}

		// ...............test..........
//		if (curtstamp == 2) {
//			System.out
//					.printf("   ++++++  CalBolt %d compute correlation between %d and %d at %f to satisfy %f",
//							locTaskId, streid1, streid2, curtstamp, thre);
//		}

		// .............................

		return tmpres <= thre ? 1 : 0;

	}

	void cellWithinCal(int cellidx, ArrayList<String> res) {

		int stre1 = 0, stre2 = 0;

		// ...............test..........
		// if (curtstamp == 2) {
		// System.out
		// .printf(" ------------------  CalBolt %d compute correlation: %d  %d\n",
		// locTaskId,cellHostStre.get(cellidx).size(),cellHostStre.get(cellidx).size()
		// );
		// }

		// .............................

		for (int i = 0; i < cellHostStre.get(cellidx).size(); i++) {
			for (int j = i + 1; j < cellHostStre.get(cellidx).size(); j++) {

				stre1 = cellHostStre.get(cellidx).get(i);
				stre2 = cellHostStre.get(cellidx).get(j);

				if (correCalDisDFT(2 - 2 * TopologyMain.thre, stre1, stre2) == 1) {

					// if (correCalDisReal(2 - 2 * TopologyMain.thre, stre1,
					// stre2) == 1)
					{

						if (stre1 > stre2)
							res.add(Integer.toString(stre2) + ","
									+ Integer.toString(stre1));
						else
							res.add(Integer.toString(stre1) + ","
									+ Integer.toString(stre2));
					}
				}

			}
		}

		for (int i = 0; i < cellHostStre.get(cellidx).size(); i++) {
			for (int j = 0; j < cellNbStre.get(cellidx).size(); j++) {

				stre1 = cellHostStre.get(cellidx).get(i);
				stre2 = cellNbStre.get(cellidx).get(j);

				if (correCalDisDFT(TopologyMain.thre, stre1, stre2) == 1) {

					// if (correCalDisReal(2 - 2 * TopologyMain.thre, stre1,
					// stre2) == 1)
					{

						if (stre1 > stre2)
							res.add(Integer.toString(stre2) + ","
									+ Integer.toString(stre1));
						else
							res.add(Integer.toString(stre1) + ","
									+ Integer.toString(stre2));
					}
				}

			}
		}

		return;
	}

	void cellCorrCal() {

		resPair.clear();
		int cellcnt = cellIdx.size();

		for (int i = 0; i < cellcnt; ++i) {

			// ...............test..........
			// if (curtstamp == 2) {
			// System.out
			// .printf(" ------------------  CalBolt %d compute correlation in cell %d\n",
			// locTaskId,cellHostStre.get(i).size());
			// }

			// .............................

			cellWithinCal(i, resPair);
		}
		return;
	}

	public void localIdxRenew() {

		cellIdx.clear();
		streIdx.clear();

		cellHostStre.clear();
		cellNbStre.clear();
		resPair.clear();

		return;
	}

	@Override
	public void cleanup() {

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

		declarer.declare(new Fields("ts", "pair"));
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {

		locTaskId = context.getThisTaskId();

		return;
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub

		// declarer.declareStream("streamData", new Fields("ts", "streId",
		// "cellCoor", "strevec","dftvec", "hostFlag"));
		//
		// declarer.declareStream("calCommand", new Fields("command",
		// "taskid"));

		streType = input.getSourceStreamId();

		if (streType.compareTo("streamData") == 0) {
			
			recStreCnt++;

			ts = input.getDoubleByField("ts");
			String strestr = input.getStringByField("strevec");
			String dftstr = input.getStringByField("dftvec");
			String cellstr = input.getStringByField("cellCoor");
			int streid = input.getIntegerByField("streId");
			int hostflag = input.getIntegerByField("hostFlag");

			if (Math.abs(curtstamp - ts) <= 1e-3) {
				IndexingStre(streid, strestr, dftstr, cellstr, hostflag);
			}

		} else if (streType.compareTo("calCommand") == 0) {

			commandStr = input.getStringByField("command");
			preTaskId = input.getLongByField("taskid");

			// // .............test..........
			
			
			
			// System.out.printf("???  compatation is performed \n");
			//
			// // ...........................

			preTaskIdx.add(preTaskId);
			if (preTaskIdx.size() < TopologyMain.preBoltNum) {
				return;
			}

			// .............test..........
//			System.out
//					.printf("??? at %f,   CalBolt %d receives: %d streams, %d cells \n",
//							curtstamp, locTaskId, streIdx.size(),
//							cellIdx.size());
//
			if (curtstamp == 2) {
				
				
				
				System.out.printf("  ----------  CalBolt %d has totally %d streams \n",locTaskId,recStreCnt);
				recStreCnt=0;

//				for (int i = 0; i < cellIdx.size(); ++i) {
//					System.out
//							.printf("??? at %f,   CalBolt %d has %d, %d streams in cell %d \n",
//									curtstamp, locTaskId,
//									cellHostStre.get(i).size(),cellNbStre.get(i).size(),i);
//				}

			}

			// ...........................

			cellCorrCal();

			// .............test..........
			// System.out.printf("???  qualified streams at %f: %d \n",
			// curtstamp, resPair.size());

			// ...........................

			for (String pair : resPair) {
				collector.emit(new Values(curtstamp, pair));
			}

			localIdxRenew();

			curtstamp = ts + 1;
			preTaskIdx.clear();
		}

		return;
	}
}
