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

public class robCalBolt extends BaseBasicBolt {

	double curtstamp = TopologyMain.winSize - 1;

	Double[][] vecData = new Double[TopologyMain.nstreBolt + 10][TopologyMain.winSize + 10];

	public double[] cursqrexp = new double[TopologyMain.nstreBolt + 10],
			curexp = new double[TopologyMain.nstreBolt + 10];

	public int[] vecst = new int[TopologyMain.nstreBolt + 10];
	public int[] veced = new int[TopologyMain.nstreBolt + 10];
	public int queueLen = TopologyMain.winSize + 5;

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

	int locTaskId;
	
	public double disThre = 2 - 2 * TopologyMain.thre;
	public double cellEps = Math.sqrt(disThre);

	int correCalDis(double thre, int streid1, int streid2, int len) {
		double dis = 0.0, tmp = 0.0;

		for (int i = 0; i < len; ++i) {
			tmp += ((vecData[streid1][i] - vecData[streid2][i]) * (vecData[streid1][i] - vecData[streid2][i]));
		}
		dis = tmp;

		return (dis <= 2 - 2 * thre) ? 1 : 0;
	}

	public void updateStreVec(int memidx, String vecdata) {

		int len = vecdata.length(), cnt = 0, pre = 0;
		vecst[memidx] = 0;
		veced[memidx] = 0;

		double exp = 0.0, sqrexp = 0.0, tmpval = 0.0;

		for (int i = 0; i < len; ++i) {
			if (vecdata.charAt(i) == ',') {

				tmpval = Double.valueOf(vecdata.substring(pre, i));

				vecData[memidx][veced[memidx]] = tmpval;
				veced[memidx] = (veced[memidx] + 1) % queueLen;

				exp = tmpval + exp;
				sqrexp = tmpval * tmpval + sqrexp;

				pre = i + 1;
			}
		}

		cursqrexp[memidx] = sqrexp / TopologyMain.winSize;
		curexp[memidx] = exp / TopologyMain.winSize;

		return;
	}

	public void updateStreData(int memidx, String vecdata) {

		double newval = Double.valueOf(vecdata), oldval = vecData[memidx][vecst[memidx]];

		vecData[memidx][veced[memidx]] = newval;

		cursqrexp[memidx] = cursqrexp[memidx] - oldval * oldval
				/ TopologyMain.winSize + newval * newval / TopologyMain.winSize;

		curexp[memidx] = curexp[memidx] - oldval / TopologyMain.winSize
				+ newval / TopologyMain.winSize;

		veced[memidx] = (veced[memidx] + 1) % queueLen;
		vecst[memidx] = (vecst[memidx] + 1) % queueLen;

		return;
	}

	public void IndexingStre(int streid, String vecdata, int taskcoor) {

		int preset = 1 - curSet, memidx = 0;

		if (streSet.get(preset).contains(streid) == false) {
			memidx = avaiMem.first();
			avaiMem.remove(memidx);
			streMem.put(streid, memidx);

			updateStreVec(memidx, vecdata);

		} else {

			memidx = streMem.get(streid);
			updateStreData(memidx, vecdata);

			streSet.get(preset).remove(streid);

		}

		streTaskCoor[memidx] = taskcoor;
		streSet.get(curSet).add(streid);

		for (int iter : streSet.get(preset)) {
			avaiMem.add(streMem.get(iter));
			streMem.remove(iter);
		}

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

		int stre = 0, memidx = 0, k = 0, cellmemidx = 0, cellcoor = 0, veccnt = 0;
		double var = 0.0, mean = 0.0, normcoor=0.0;
		String cellstr = new String();

		int[] tmpvec = new int[TopologyMain.winSize + 5];

		for (Map.Entry<Integer, Integer> entry : streMem.entrySet()) {

			stre = entry.getKey();
			memidx = entry.getValue();

			k = vecst[memidx];
			var = (cursqrexp[memidx] - curexp[memidx]) * TopologyMain.winSize;
			mean = curexp[memidx];
			veccnt = 0;

			if (Math.abs(var - 0.0) < 1e-6) {
				continue;
			}

			while (k != veced[memidx]) {
				
				normcoor = (int) ((vecData[memidx][k] - mean) / Math.sqrt(var));
				
				if (normcoor>=0)
				{
					cellcoor =(int) Math.floor(normcoor/cellEps);
				}
				else
				{
					cellcoor =-1*(int)Math.ceil(-1*normcoor/cellEps);
				}
				
				cellstr = Integer.toString(cellcoor) + ",";
				tmpvec[veccnt++] = cellcoor;

				k = (k + 1) % queueLen;
			}

			cellmemidx = cellMemAlloc(cellstr, tmpvec);

			if (streTaskCoor[memidx] == locTaskId) {
				cellHostStre.get(cellmemidx).add(stre);
			} else {
				cellNbStre.get(cellmemidx).add(stre);
			}

		}

		return;
	}

	int cellVecCheck(int idx1, int idx2) {
		for (int j = 0; j < TopologyMain.winSize; ++j) {
			if (Math.abs(cellVecs[idx1][j]-cellVecs[idx2][j])>1 ) {
				return 0;
			}
		}
		return 1;
	}

	void corCal() {

		for (int i = 0; i < cellIdxcnt; ++i) {
			for (int j = i + 1; j < cellIdxcnt; ++j) {
				if (cellVecCheck(i, j) == 1) {

				}

			}
		}

		return;
	}

	public void localIdxRenew() {

		// // stridcnt = 0;
		// // stridMap.clear();
		//
		// gridIdx.clear();
		// gridIdxcnt = 0;
		//
		// gridStre.clear();
		// qualPair.clear();

		return;
	}

//	public int correGrid(int gridNo, double thre,
//			BasicOutputCollector collector, ArrayList<String> resPair) {
//
//		int rescnt = 0, i = 0, j = 0, tmpcnt = gridStre.get(gridNo).size();
//		int stre1 = 0, stre2 = 0;
//
//		String Pair = new String();
//
//		for (i = 0; i < tmpcnt; ++i) {
//
//			stre1 = gridStre.get(gridNo).get(i);
//
//			for (j = i + 1; j < tmpcnt; ++j) {
//
//				stre2 = gridStre.get(gridNo).get(j);
//
//				// if (strlocal[stre1] == 0 && strlocal[stre2] == 0) {
//				// } else {
//
//				if ((strlocal[stre1] == 1 && strlocal[stre2] == 0)
//						|| (strlocal[stre1] == 0 && strlocal[stre2] == 1)) {
//
//					if (correCalDis(thre, stre1, stre2, TopologyMain.winSize) == 1) {
//
//						if (strid[stre1] > strid[stre2]) {
//
//							Pair = (Integer.toString(strid[stre2]) + "," + Integer
//									.toString(strid[stre1]));
//
//							rescnt++;
//							resPair.add(Pair);
//
//						} else {
//
//							Pair = (Integer.toString(strid[stre1]) + "," + Integer
//									.toString(strid[stre2]));
//
//							rescnt++;
//							resPair.add(Pair);
//
//						}
//					}
//				}
//
//			}
//		}
//
//		return rescnt;
//	}

	@Override
	public void cleanup() {
		// System.out.println("-- Word Counter [" + name + "-" + id + "] --");
		// for (Map.Entry<String, Integer> entry : counters.entrySet()) {
		// System.out.println(entry.getKey() + ": " + entry.getValue());
		// }

		// ....output stream........

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

		locTaskId = context.getThisTaskIndex();

		return;
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub

		// declarer.declare(new Fields("repType", "strevec", "ts", "taskCoor",
		// "streId", "gap"));

		double ts = input.getDoubleByField("ts");
		String vecstr = input.getStringByField("strevec");
		int streid = input.getIntegerByField("streId");
		int taskid = input.getIntegerByField("taskCoor");

		if (ts > curtstamp) {

//			vecBatchAna();
//			for (int i = 0; i < gridIdxcnt; ++i) {
//				qualPair.clear();
//				correGrid(i, TopologyMain.thre, collector, qualPair);
//
//				Iterator<String> it = qualPair.iterator();
//				String tmp = new String();
//				while (it.hasNext()) {
//					tmp = it.next();
//					collector.emit(new Values(curtstamp, tmp));
//				}
//			}

			localIdxRenew();

			curSet = 1 - curSet;
			IndexingStre(streid, vecstr, taskid);
			curtstamp = ts;

		} else if (ts < curtstamp) {
			System.out.printf("!!!!!!!!!!!!! time sequence disorder\n");
		} else if (Math.abs(ts - curtstamp) <= 1e-3) {

			IndexingStre(streid, vecstr, taskid);

		}
		return;
	}
}
