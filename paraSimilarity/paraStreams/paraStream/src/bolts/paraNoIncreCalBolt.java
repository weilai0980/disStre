package bolts;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import main.TopologyMain;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class paraNoIncreCalBolt extends BaseBasicBolt {

	// ............input time order..............//

	double curtstamp = TopologyMain.winSize - 1;
	String streType = new String();
	double ts = 0.0;

	String commandStr = new String();
	long preTaskId = 0;

	// .........memory management for sliding windows.................//

	double[][] vecData = new double[TopologyMain.nstreBolt + 10][TopologyMain.winSize + 10];

	double[] cursqexp = new double[TopologyMain.nstreBolt + 10],
			curexp = new double[TopologyMain.nstreBolt + 10];

	int[] vecst = new int[TopologyMain.nstreBolt + 10];
	int[] veced = new int[TopologyMain.nstreBolt + 10];
	int queueLen = TopologyMain.winSize + 5;

	Integer[] streTaskCoor = new Integer[TopologyMain.nstreBolt + 10];

	ArrayList<Set<Integer>> streSet = new ArrayList<Set<Integer>>();
	Set<Integer> set1 = new HashSet<Integer>();
	Set<Integer> set2 = new HashSet<Integer>();
	int curSet = 0;

	TreeSet<Integer> avaiMem = new TreeSet<Integer>();
	HashMap<Integer, Integer> streMem = new HashMap<Integer, Integer>();

	HashMap<String, Integer> cellIdx = new HashMap<String, Integer>();
	int cellIdxcnt = 0;
	List<List<Integer>> cellHostStre = new ArrayList<List<Integer>>(
			TopologyMain.gridIdxN + 5);
	List<List<Integer>> cellNbStre = new ArrayList<List<Integer>>(
			TopologyMain.gridIdxN + 5);
	int[][] cellVecs = new int[TopologyMain.gridIdxN + 5][TopologyMain.winSize + 5];
	ArrayList<String> resPair = new ArrayList<String>();

	// ...........computation parameter....................//
	int locTaskId;
	double disThre = 2 - 2 * TopologyMain.thre;
	double cellEps = Math.sqrt(disThre);
	HashSet<Long> preTaskIdx = new HashSet<Long>();

	public void updateStreVec(int memidx, String vecdata) {

		int len = vecdata.length(), pre = 0;
		vecst[memidx] = 0;
		veced[memidx] = 0;

		double exp = 0.0, sqexp = 0.0, tmpval = 0.0;

		for (int i = 0; i < len; ++i) {
			if (vecdata.charAt(i) == ',') {

				tmpval = Double.valueOf(vecdata.substring(pre, i));

				vecData[memidx][veced[memidx]] = tmpval;
				veced[memidx] = (veced[memidx] + 1) % queueLen;

				exp = tmpval + exp;
				sqexp = tmpval * tmpval + sqexp;

				pre = i + 1;
			}
		}

		cursqexp[memidx] = sqexp / TopologyMain.winSize;
		curexp[memidx] = exp / TopologyMain.winSize;

		return;
	}

	public void updateStreData(int memidx, String vecdata) {

		double newval = Double.valueOf(vecdata);
		double oldval = vecData[memidx][vecst[memidx]];

		vecData[memidx][veced[memidx]] = newval;

		cursqexp[memidx] = cursqexp[memidx] - oldval * oldval
				/ TopologyMain.winSize + newval * newval / TopologyMain.winSize;

		curexp[memidx] = curexp[memidx] - oldval / TopologyMain.winSize
				+ newval / TopologyMain.winSize;

		veced[memidx] = (veced[memidx] + 1) % queueLen;
		vecst[memidx] = (vecst[memidx] + 1) % queueLen;

		return;
	}

	public void IndexingStre(int streid, String vecdata, int taskcoor) {

		int preset = 1 - curSet, memidx = 0;

		if (streSet.get(curSet).contains(streid) == true) {
			return;
		}

		if (streSet.get(preset).contains(streid) == false) {
			memidx = avaiMem.first();
			avaiMem.remove(memidx);

			streMem.put(streid, memidx);

			updateStreVec(memidx, vecdata);

		} else {

			// ..........test..........
			// System.out.printf("at time %f,in calBolt %d, stream %d with data %s\n ",curtstamp,locTaskId,
			// streid, vecdata);

			// ........................

			memidx = streMem.get(streid);
			updateStreData(memidx, vecdata);

			streSet.get(preset).remove(streid);

		}

		streTaskCoor[memidx] = taskcoor;
		streSet.get(curSet).add(streid);

		return;
	}

	int cellMemAlloc(String cellstr, int cellvec[]) {

		if (cellIdx.containsKey(cellstr) == true) {

			return cellIdx.get(cellstr);

		} else {

			cellIdx.put(cellstr, cellIdxcnt);

			for (int i = 0; i < TopologyMain.winSize; ++i) {
				cellVecs[cellIdxcnt][i] = cellvec[i];
			}

			cellIdxcnt++;

			cellHostStre.add(new ArrayList<Integer>());
			cellNbStre.add(new ArrayList<Integer>());

			return cellIdxcnt - 1;
		}
	}

	void cellStreamMapping() {

		int memidx = 0, k = 0, cellmemidx = 0, cellcoor = 0, veccnt = 0;
		double denomi = 0.0, mean = 0.0, normcoor = 0.0;
		String cellstr = new String();

		int[] tmpvec = new int[TopologyMain.winSize + 5];

		for (Integer stre : streSet.get(curSet)) {

			memidx = streMem.get(stre);
			cellstr = "";

			k = vecst[memidx];
			denomi = (cursqexp[memidx] - curexp[memidx] * curexp[memidx])
					* TopologyMain.winSize;
			mean = curexp[memidx];
			veccnt = 0;

			if (Math.abs(denomi - 0.0) < 1e-6) {
				continue;
			}

			while (k != veced[memidx]) {

				normcoor = ((vecData[memidx][k] - mean) / Math.sqrt(denomi));

				if (normcoor >= 0) {
					cellcoor = (int) Math.floor(normcoor / cellEps);
				} else {
					cellcoor = -1 * (int) Math.ceil(-1 * normcoor / cellEps);
				}

				cellstr = cellstr + Integer.toString(cellcoor) + ",";
				tmpvec[veccnt++] = cellcoor;

				k = (k + 1) % queueLen;
			}

			cellmemidx = cellMemAlloc(cellstr, tmpvec);

			if (streTaskCoor[memidx] == locTaskId) {
				cellHostStre.get(cellmemidx).add(stre);
			} else {
				cellNbStre.get(cellmemidx).add(stre);
			}

			// ...............test.....................
//			if (curtstamp == 5 && locTaskId == 8 && (stre == 14 || stre == 16)) {
//
//				System.out
//						.printf(" ???????? at time %f, calBolt %d locates stream %d in cell %s with index %d \n",
//								curtstamp, locTaskId, stre, cellstr, cellmemidx);
//
//			}
			// .......................................

		}

		return;
	}

	int cellVecCheck(int idx1, int idx2) {

		for (int j = 0; j < TopologyMain.winSize; ++j) {
			if (Math.abs(cellVecs[idx1][j] - cellVecs[idx2][j]) > 1) {
				return 0;
			}
		}
		return 1;
	}

	double correCalDis(int streid1, int streid2) {

		int memidx1 = streMem.get(streid1), memidx2 = streMem.get(streid2);

		int k1 = vecst[memidx1], k2 = vecst[memidx2];
		double deno1 = Math.sqrt((cursqexp[memidx1] - curexp[memidx1]
				* curexp[memidx1])
				* TopologyMain.winSize), mean1 = curexp[memidx1];
		double deno2 = Math.sqrt((cursqexp[memidx2] - curexp[memidx2]
				* curexp[memidx2])
				* TopologyMain.winSize), mean2 = curexp[memidx2];

		double tmpres = 0.0;

		while (k2 != veced[memidx2]) {

			tmpres = tmpres + (vecData[memidx1][k1] - mean1)
					* (vecData[memidx2][k2] - mean2);

			k2 = (k2 + 1) % queueLen;
			k1 = (k1 + 1) % queueLen;
		}

		tmpres = tmpres / (deno1 * deno2);

		return tmpres;

	}

	void cellWithinCal(int cellidx, ArrayList<String> res) {

		int stre1 = 0, stre2 = 0;
		double tmpcor = 0.0;
		for (int i = 0; i < cellHostStre.get(cellidx).size(); i++) {
			for (int j = i + 1; j < cellHostStre.get(cellidx).size(); j++) {

				stre1 = cellHostStre.get(cellidx).get(i);
				stre2 = cellHostStre.get(cellidx).get(j);

				// ...............test.....................
//				if (curtstamp == 5
//						&& locTaskId == 8
//						&& ((stre1 == 14 && stre2 == 16) || (stre1 == 16 && stre2 == 14))) {
//
//					System.out
//							.printf(" ???????? at time %f, calBolt %d computes streams between %d and %d with correlation %f  \n",
//									curtstamp, locTaskId, stre1, stre2,
//									correCalDis(stre1, stre2));
//
//				}
				// .......................................

				if ((tmpcor = correCalDis(stre1, stre2)) >= TopologyMain.thre) {

					// if (stre1 > stre2)
					// res.add(Integer.toString(stre2) + ","
					// + Integer.toString(stre1));
					// else
					res.add(Integer.toString(stre1) + ","
							+ Integer.toString(stre2) + ","
							+ Double.toString(tmpcor));
				}

			}
		}

		for (int i = 0; i < cellHostStre.get(cellidx).size(); i++) {
			for (int j = 0; j < cellNbStre.get(cellidx).size(); j++) {

				stre1 = cellHostStre.get(cellidx).get(i);
				stre2 = cellNbStre.get(cellidx).get(j);

				// ...............test.....................
//				if (curtstamp == 5
//						&& locTaskId == 8
//						&& ((stre1 == 14 && stre2 == 16) || (stre1 == 16 && stre2 == 14))) {
//
//					System.out
//							.printf(" ???????? at time %f, calBolt %d computes streams between %d and %d with correlation %f  \n",
//									curtstamp, locTaskId, stre1, stre2,
//									correCalDis(stre1, stre2));
//
//				}
				// .......................................

				if ((tmpcor = correCalDis(stre1, stre2)) >= TopologyMain.thre) {

					// if (stre1 > stre2)
					// res.add(Integer.toString(stre2) + ","
					// + Integer.toString(stre1));
					// else
					res.add(Integer.toString(stre1) + ","
							+ Integer.toString(stre2) + ","
							+ Double.toString(tmpcor));
				}

			}
		}

		return;
	}

	void cellInterCal(int cellidx1, int cellidx2, ArrayList<String> res) {

		int stre1 = 0, stre2 = 0, stre3 = 0, ini = 1;
		double tmpcor = 0.0;

		// ...............test.....................
		// if (curtstamp == 5
		// && locTaskId == 8){
		// // && ((stre1 == 14 && stre2 == 16) || (stre1 == 16 && stre2 == 14)))
		// {
		//
		// System.out
		// .printf(" ???????? at time %f, calBolt %d computes cells with size: %d %d %d %d  \n",
		// curtstamp, locTaskId, cellHostStre.get(cellidx1).size(),
		// cellNbStre.get(cellidx1).size(), cellHostStre.get(cellidx2).size(),
		// cellNbStre.get(cellidx2).size());
		//
		// }
		// .......................................

		for (int i = 0; i < cellHostStre.get(cellidx1).size(); i++) {

			stre1 = cellHostStre.get(cellidx1).get(i);

			for (int j = 0; j < cellHostStre.get(cellidx2).size(); j++) {

				stre2 = cellHostStre.get(cellidx2).get(j);

				// ...............test.....................
//				if (curtstamp == 5 && locTaskId == 8) {
//					// && ((stre1 == 14 && stre2 == 16) || (stre1 == 16 && stre2
//					// == 14))) {
//
//					System.out
//							.printf(" ???????? at time %f, calBolt %d computes streams between %d and %d with correlation %f  \n",
//									curtstamp, locTaskId, stre1, stre2,
//									correCalDis(stre1, stre2));
//
//				}
				// .......................................

				if ((tmpcor = correCalDis(stre1, stre2)) >= TopologyMain.thre) {

					// if (stre1 > stre2)
					// res.add(Integer.toString(stre2) + ","
					// + Integer.toString(stre1));
					// else
					res.add(Integer.toString(stre1) + ","
							+ Integer.toString(stre2) + ","
							+ Double.toString(tmpcor));
				}

			}

			for (int j = 0; j < cellNbStre.get(cellidx2).size(); j++) {

				stre1 = cellHostStre.get(cellidx1).get(i);
				stre2 = cellNbStre.get(cellidx2).get(j);

				// ...............test.....................
//				if (curtstamp == 5 && locTaskId == 8) {
//					// && ((stre1 == 14 && stre2 == 16) || (stre1 == 16 && stre2
//					// == 14))) {
//
//					System.out
//							.printf(" ???????? at time %f, calBolt %d computes streams between %d and %d with correlation %f  \n",
//									curtstamp, locTaskId, stre1, stre2,
//									correCalDis(stre1, stre2));
//
//				}
				// .......................................

				if ((tmpcor = correCalDis(stre1, stre2)) >= TopologyMain.thre) {

					// if (stre1 > stre2)
					// res.add(Integer.toString(stre2) + ","
					// + Integer.toString(stre1));
					// else
					res.add(Integer.toString(stre1) + ","
							+ Integer.toString(stre2) + ","
							+ Double.toString(tmpcor));
				}
			}

		}

		for (int i = 0; i < cellHostStre.get(cellidx2).size(); i++) {
			for (int j = 0; j < cellNbStre.get(cellidx1).size(); j++) {
				
				stre1 = cellHostStre.get(cellidx2).get(i);
				stre2 = cellNbStre.get(cellidx1).get(j);
				
				if ((tmpcor = correCalDis(stre1, stre2)) >= TopologyMain.thre) {

					// if (stre1 > stre2)
					// res.add(Integer.toString(stre2) + ","
					// + Integer.toString(stre1));
					// else
					res.add(Integer.toString(stre1) + ","
							+ Integer.toString(stre2) + ","
							+ Double.toString(tmpcor));
				}	
			}
		}

		return;
	}

	void cellCorrCal() {

		resPair.clear();

		for (int i = 0; i < cellIdxcnt; ++i) {

			cellWithinCal(i, resPair);
			for (int j = i + 1; j < cellIdxcnt; ++j) {

				if (cellVecCheck(i, j) == 1) {

					// ..............test..................

//					if (curtstamp == 5 && locTaskId == 8 && (i == 2 && j == 8)) {
//
//						System.out
//								.printf(" ???????? at time %f, calBolt %d computes   \n",
//										curtstamp, locTaskId);
//
//					}

					// ....................................

					cellInterCal(i, j, resPair);
				}
			}
		}
		return;
	}

	public void localIdxRenew() {

		cellIdx.clear();
		cellIdxcnt = 0;

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

		streSet.add(set1);
		streSet.add(set2);

		streSet.get(0).clear();
		streSet.get(1).clear();

		streMem.clear();

		locTaskId = context.getThisTaskIndex();

		for (int i = 0; i < TopologyMain.nstreBolt + 10; ++i) {
			avaiMem.add(i);
		}

		return;
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub

		// declarer.declare(new Fields("repType", "strevec", "ts", "taskCoor",
		// "streId", "gap"));

		streType = input.getSourceStreamId();

		if (streType.compareTo("streamData") == 0) {

			ts = input.getDoubleByField("ts");
			String vecstr = input.getStringByField("strevec");
			int streid = input.getIntegerByField("streId");
			int taskid = input.getIntegerByField("taskCoor");

			// ...........test........
//			if (curtstamp == 5 && locTaskId == 8
//					&& (streid == 14 || streid == 16)) {
//				System.out
//						.printf(" !!!!!!!!! at time %f, calBolt %d gets stream %d with taskId %d and data %s  \n",
//								curtstamp, locTaskId, streid, taskid, vecstr);
//			}

			// .......................

			// if (Math.abs(curtstamp - ts) <= 1e-3) {
			IndexingStre(streid, vecstr, taskid);
			// }

		} else if (streType.compareTo("calCommand") == 0) {

			commandStr = input.getStringByField("command");
			preTaskId = input.getLongByField("taskid");

			preTaskIdx.add(preTaskId);
			if (preTaskIdx.size() < TopologyMain.preBoltNum) {
				return;
			}

			// ...........test...............

			// if (curtstamp == 5) {
			// //
			// System.out.printf(
			// "at time %f, calBolt %d has streams: %d\n",
			// curtstamp, locTaskId, streSet.get(curSet).size());
			//
			// // for (int i : streSet.get(curSet)) {
			// //
			// System.out.printf("stream %d is on position: %d  with host task %d\n",
			// i,
			// // streMem.get(i),streTaskCoor[streMem.get(i)]);
			// //
			// }
			// }

			// ..............................

			int preset = 1 - curSet;
			for (int iter : streSet.get(preset)) {
				avaiMem.add(streMem.get(iter));
				streMem.remove(iter);
			}

			cellStreamMapping();
			cellCorrCal();

			for (String pair : resPair) {
				collector.emit(new Values(curtstamp, pair));
			}
			localIdxRenew();

			curSet = 1 - curSet;
			streSet.get(curSet).clear();

			curtstamp = ts + 1;
			preTaskIdx.clear();
		}

		return;
	}
}
