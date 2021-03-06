package bolts;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import main.TopologyMain;
import tools.streamPair;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


public class AdjustApproBolt extends BaseBasicBolt {

	// ..........time order...........//
	double curtstamp = TopologyMain.winSize - 1;
	double ts = 0.0;
	String streType = new String();
	HashSet<Long> preTaskIdx = new HashSet<Long>();
	String commandStr = new String();
	long preTaskId = 0;
	long srctask = 0;
	// ..........memory................//
	int[][] gridCoors = new int[TopologyMain.gridIdxN + 5][TopologyMain.winSize + 5];
	double[][] pivotVec = new double[TopologyMain.gridIdxN + 5][TopologyMain.winSize + 5];

	int[] srcTaskId = new int[TopologyMain.gridIdxN + 5];

	int[] gridPivot = new int[TopologyMain.gridIdxN + 5];
	int[] gridBolt = new int[TopologyMain.gridIdxN + 5];
	int[] gpTaskId = new int[TopologyMain.gridIdxN + 5];

	int gridIdxcnt = 0;

	List<List<Double>> gridAffs = new ArrayList<List<Double>>(
			TopologyMain.nstreBolt + 5);

	List<List<Integer>> gridAdjIdx = new ArrayList<List<Integer>>(
			TopologyMain.nstreBolt + 5);

	HashMap<Integer, Integer> pStreMap = new HashMap<Integer, Integer>();

	int sentinel = -100000;
	int boltNo = 0;

	public static int glAppBolt = 0;
	public int locAppBolt = 0;
	
	HashMap<Integer,Integer > retriStre=new HashMap<Integer,Integer >();
	HashSet<streamPair> retriPair=new HashSet<streamPair>();
	

	// .........computation parameters...............//

	public double subDivNum = 2;
	public double taskRange = 2.0 / subDivNum;
	public double disThre = 2 - 2 * TopologyMain.thre;
	public int gridRange = (int) Math.ceil(1.0 / Math.sqrt(disThre));
	// public int taskGridMax=2*gridRange-1;
	public double taskGridCap = taskRange / Math.sqrt(disThre);
	public int locTaskId;

	// ...........test...................//
	// public int ta = 3, tb = 8;
	// public double tt = 21;

	// ...................................//

	public int groupTaskId(int cell[][], int id, int dimN) {

		int cellCoor = 0, partCoor = 0;
		int tmpid = 0;

		cellCoor = cell[id][0] + gridRange + 1;
		tmpid = (int) Math.ceil((double) cellCoor / taskGridCap);

		for (int i = 1; i < dimN; ++i) {

			cellCoor = cell[id][i] + gridRange + 1; // coordinate
			// transposition to
			// positive range
			partCoor = (int) Math.ceil((double) cellCoor / taskGridCap);

			tmpid = (int) (tmpid + (partCoor - 1) * Math.pow(subDivNum, i));
		}

		return tmpid - 1;
	}

	public int pivotVecAna(String orgstr, double vecval[][], int id) {

		int len = orgstr.length();
		int cnt = 0, pre = 0;
		for (int i = 0; i < len; ++i) {
			if (orgstr.charAt(i) == ',') {
				vecval[id][cnt++] = Double.valueOf(orgstr.substring(pre, i));
				pre = i + 1;
			}
		}
		return cnt;
	}

	public int pivotCoor(String orgstr, int coor[][], int id) {

		int len = orgstr.length();
		int cnt = 0, pre = 0;
		for (int i = 0; i < len; ++i) {
			if (orgstr.charAt(i) == ',') {
				coor[id][cnt++] = Integer.valueOf(orgstr.substring(pre, i));
				pre = i + 1;
			}
		}

		return cnt;
	}

	public int pivotAffs(String orgstr, double affines[][], int id) {

		int len = orgstr.length();
		int cnt = 0, pre = 0;
		for (int i = 0; i < len; ++i) {
			if (orgstr.charAt(i) == ',' || orgstr.charAt(i) == ';') {
				affines[id][cnt++] = Double.valueOf(orgstr.substring(pre, i));
				pre = i + 1;
			}
		}
		affines[id][cnt++] = sentinel;
		return cnt;
	}

	public int pivotAffs(String orgstr, int id) {

		int len = orgstr.length();
		int cnt = 0, pre = 0;
		for (int i = 0; i < len; ++i) {
			if (orgstr.charAt(i) == ',' || orgstr.charAt(i) == ';') {
				gridAffs.get(id).add(Double.valueOf(orgstr.substring(pre, i)));
				pre = i + 1;
			}
		}
		// gridAffs.get[id][cnt++] = sentinel;
		return cnt;
	}

	public int adjIdx(String orgstr, int adjStre[][], int id) {

		int len = orgstr.length();
		int cnt = 0, pre = 0;
		for (int i = 0; i < len; ++i) {
			if (orgstr.charAt(i) == ',') {
				adjStre[id][cnt++] = Integer.valueOf(orgstr.substring(pre, i));
				pre = i + 1;
			}
		}
		adjStre[id][cnt++] = sentinel;
		return cnt;
	}

	public int adjIdx(String orgstr, int id) {

		int len = orgstr.length();
		int cnt = 0, pre = 0;
		for (int i = 0; i < len; ++i) {
			if (orgstr.charAt(i) == ',') {
				gridAdjIdx.get(id).add(
						Integer.valueOf(orgstr.substring(pre, i)));
				// [id][cnt++] = Integer.valueOf(orgstr.substring(pre, i));
				pre = i + 1;
			}
		}
		// gridAdjIdx[id][cnt++] = sentinel;
		return cnt;
	}

	public boolean localpStreIdx(int stre) {

		if (pStreMap.containsKey(stre) == true) {
			return false;
		} else {
			pStreMap.put(stre, gridIdxcnt);
			return true;
		}

	}

	public void localIdxRenew() {

		gridAffs.clear();
		gridAdjIdx.clear();

		gridIdxcnt = 0;
		pStreMap.clear();

		return;
	}

	public int checkGrids(int idx1, int idx2) {

		if (gridBolt[idx1] == gridBolt[idx2]) {
			return 0;
		}
		if (gpTaskId[idx1] != locTaskId && gpTaskId[idx2] != locTaskId) {
			return 0;
		}

		for (int j = 0; j < TopologyMain.winSize; ++j) {
			if ((gridCoors[idx1][j] + 1) < gridCoors[idx2][j] - 1
					|| (gridCoors[idx1][j] - 1) > gridCoors[idx2][j] + 1) {
				return 0;
			}
		}
		return 1;
	}

	public void boundCheck(double tStamp, double low, double up, double thre,
			BasicOutputCollector collector, int stream1, int stream2) {

		int stre1 = (stream1 < stream2 ? stream1 : stream2);
		int stre2 = (stream1 < stream2 ? stream2 : stream1);

		if (up <= thre) {

			collector.emit(
					"interQualStre",
					new Values(tStamp, Integer.toString(stre1) + ","
							+ Integer.toString(stre2)));

		} else if (low <= thre) {

			int cnt=retriStre.size();
			
			retriStre.put(stre1,cnt);
			retriStre.put(stre2,cnt+1);
			
			streamPair tmp= new streamPair(stre1,stre2);
			retriPair.add(tmp);
			
		}

		return;
	}

	public int corBtwAffInGrids(int idx1, int idx2, double thre,
			BasicOutputCollector collector, double tStamp) {

		int i = 0, j = 0, cnt = 0, k = 0;
		double tmpdis = 0.0;
		double w11 = 0.0, w10 = 0.0, er1 = 0.0, w21 = 0.0, w20 = 0.0, er2 = 0.0;
		double[] tmpvec = new double[TopologyMain.winSize + 5];
		double tmpscal = 0.0, tmpscal2 = 0.0, tmpdis2 = 0.0;
		int iniflag = 1;
		int adjidx1 = 0, adjidx2 = 0;

		double sqrthre = Math.sqrt(thre), upbound = 0.0, lowbound = 0.0;

		Iterator<Double> it1 = gridAffs.get(idx1).iterator();

		while (it1.hasNext()) {

			adjidx2 = 0;
			j = 0;

			w11 = it1.next();
			w10 = it1.next();
			er1 = it1.next();

			tmpdis = 0.0;
			for (k = 0; k < TopologyMain.winSize; ++k) {
				tmpvec[k] = w11 * pivotVec[idx1][k] + w10;

				tmpscal = tmpvec[k] - pivotVec[idx2][k];
				// tmpscal = w11 * pivotVec[idx1][k] + w10 - pivotVec[idx2][k];

				tmpdis += (tmpscal * tmpscal);

			}
			 tmpdis = Math.abs(Math.sqrt(tmpdis) - Math.sqrt(er1));

//			lowbound = Math.abs(Math.sqrt(tmpdis) - Math.sqrt(er1));
//			upbound = Math.sqrt(tmpdis) + Math.sqrt(er1);

//			boundCheck(tStamp, lowbound, upbound, sqrthre, collector,
//					gridAdjIdx.get(idx1).get(adjidx1), gridPivot[idx2]);

			// .................
			// if (tmpdis <= sqrthre) {
			// cnt++;
			//
			// if (gridAdjIdx.get(idx1).get(adjidx1) < gridPivot[idx2]) {
			//
			// collector.emit(
			// "interQualStre",
			// new Values(tStamp, Integer.toString(gridAdjIdx.get(
			// idx1).get(adjidx1))
			// + "," + Integer.toString(gridPivot[idx2])));
			// } else {
			//
			// collector.emit(
			// "interQualStre",
			// new Values(tStamp, Integer
			// .toString(gridPivot[idx2])
			// + ","
			// + Integer.toString(gridAdjIdx.get(idx1)
			// .get(adjidx1))));
			//
			// }
			//
			// }
			// ....................

			Iterator<Double> it2 = gridAffs.get(idx2).iterator();

			while (it2.hasNext()) {

				w21 = it2.next();
				w20 = it2.next();
				er2 = it2.next();

				tmpdis = 0.0;
				tmpdis2 = 0.0;
				for (k = 0; k < TopologyMain.winSize; ++k) {
					tmpscal = tmpvec[k] - w21 * pivotVec[idx2][k] - w20;
					tmpdis += (tmpscal * tmpscal);

					if (iniflag == 1) {
						tmpscal2 = w21 * pivotVec[idx2][k] + w20
								- pivotVec[idx1][k];
						tmpdis2 += (tmpscal2 * tmpscal2);
					}

				}

				// lowbound = tmpdis;
				// upbound = Math.abs(Math.sqrt(tmpdis) + Math.sqrt(er1));

				upbound = Math.sqrt(tmpdis) + Math.sqrt(er1) + Math.sqrt(er2);

				// tmpdis = Math.sqrt(tmpdis) - Math.sqrt(er1) - Math.sqrt(er2);

				lowbound = Math.max(
						Math.sqrt(tmpdis) - Math.sqrt(er1) - Math.sqrt(er2),
						-Math.sqrt(tmpdis)
								+ Math.abs(Math.sqrt(er1) - Math.sqrt(er2)));

				boundCheck(tStamp, lowbound, upbound, sqrthre, collector,
						gridAdjIdx.get(idx1).get(adjidx1), gridAdjIdx.get(idx2)
								.get(adjidx2));

				// if (tmpdis <= sqrthre) {
				// cnt++;
				//
				// if (gridAdjIdx.get(idx1).get(adjidx1) < gridAdjIdx
				// .get(idx2).get(adjidx2)) {
				//
				// collector.emit(
				// "interQualStre",
				// new Values(tStamp, Integer.toString(gridAdjIdx
				// .get(idx1).get(adjidx1))
				// + ","
				// + Integer.toString(gridAdjIdx.get(idx2)
				// .get(adjidx2))));
				// } else {
				//
				// collector.emit(
				// "interQualStre",
				// new Values(tStamp, Integer.toString(gridAdjIdx
				// .get(idx2).get(adjidx2))
				// + ","
				// + Integer.toString(gridAdjIdx.get(idx1)
				// .get(adjidx1))));
				//
				// }
				//
				// }

				if (iniflag == 1) {

					lowbound = Math.abs(Math.sqrt(tmpdis2) - Math.sqrt(er2));
					upbound = Math.sqrt(tmpdis2) + Math.sqrt(er2);

					boundCheck(tStamp, lowbound, upbound, sqrthre, collector,
							gridAdjIdx.get(idx2).get(adjidx2), gridPivot[idx1]);

					// if (tmpdis2 <= sqrthre) {
					//
					// cnt++;
					//
					// if (gridPivot[idx1] < gridAdjIdx.get(idx2).get(adjidx2))
					// {
					// collector.emit(
					// "interQualStre",
					// new Values(tStamp, Integer
					// .toString(gridPivot[idx1])
					// + ","
					// + Integer.toString(gridAdjIdx.get(
					// idx2).get(adjidx2))));
					// } else {
					// collector
					// .emit("interQualStre",
					// new Values(
					// tStamp,
					// Integer.toString(gridAdjIdx
					// .get(idx2).get(
					// adjidx2))
					// + ","
					// + Integer
					// .toString(gridPivot[idx1])));
					//
					// }
					//
					// }
				}

				j = j + 3;
				adjidx2++;
			}

			adjidx1++;
			i = i + 3;

			iniflag = 0;
		}

		if (adjidx1 == 0) {

			j = 0;

			Iterator<Double> it2 = gridAffs.get(idx2).iterator();

			while (it2.hasNext()) {

				w21 = it2.next();
				w20 = it2.next();
				er2 = it2.next();

				tmpdis2 = 0.0;
				for (k = 0; k < TopologyMain.winSize; ++k) {

					tmpscal2 = w21 * pivotVec[idx2][k] + w20
							- pivotVec[idx1][k];
					tmpdis2 += (tmpscal2 * tmpscal2);

				}

				// tmpdis2 = Math.abs(Math.sqrt(tmpdis2) - Math.sqrt(er2));

				lowbound = Math.abs(Math.sqrt(tmpdis2) - Math.sqrt(er2));
				upbound = Math.sqrt(tmpdis2) + Math.sqrt(er2);

				boundCheck(tStamp, lowbound, upbound, sqrthre, collector,
						gridAdjIdx.get(idx2).get(adjidx2), gridPivot[idx1]);

				//
				// if (tmpdis2 <= sqrthre) {
				// cnt++;
				//
				// if (gridPivot[idx1] < gridAdjIdx.get(idx2).get(adjidx2)) {
				// collector.emit(
				// "interQualStre",
				// new Values(tStamp, Integer
				// .toString(gridPivot[idx1])
				// + ","
				// + Integer.toString(gridAdjIdx.get(idx2)
				// .get(adjidx2))));
				// } else {
				// collector.emit(
				// "interQualStre",
				// new Values(tStamp, Integer.toString(gridAdjIdx
				// .get(idx2).get(adjidx2))
				// + ","
				// + Integer.toString(gridPivot[idx1])));
				//
				// }
				//
				// }
				j = j + 3;
				adjidx2++;
			}

		}

		return cnt;
	}

	public int corBtwPivots(int idx1, int idx2, double sqthre,
			BasicOutputCollector collector, double tStamp) {

		int k = 0;
		double tmpscal = 0.0, tmpdis = 0.0;
		for (k = 0; k < TopologyMain.winSize; ++k) {
			tmpscal = pivotVec[idx1][k] - pivotVec[idx2][k];
			tmpdis += (tmpscal * tmpscal);

		}

		if (tmpdis <= sqthre) {
			if (gridPivot[idx1] < gridPivot[idx2]) {
				collector.emit("interQualStre",
						new Values(tStamp, Integer.toString(gridPivot[idx1])
								+ "," + Integer.toString(gridPivot[idx2])));
			} else {
				collector.emit("interQualStre",
						new Values(tStamp, Integer.toString(gridPivot[idx2])
								+ "," + Integer.toString(gridPivot[idx1])));

			}
			return 1;
		} else {
			return 0;
		}
	}

	@Override
	public void cleanup() {

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

		declarer.declareStream("interQualStre", new Fields("ts", "pair"));

		declarer.declareStream("retriStre", new Fields("ts", "streId"));

	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {

		locAppBolt = glAppBolt++;
		locTaskId = context.getThisTaskIndex();

		return;
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub

		streType = input.getSourceStreamId();

		if (streType.compareTo("streamData") == 0) {
			int tmpid = 0;

			ts = input.getDoubleByField("ts");
			String coordstr = input.getStringByField("coord");
			String pivotstr = input.getStringByField("pivotvec");
			int pivotId = input.getIntegerByField("pidx");
			String affRelStr = input.getStringByField("adjaffine");
			String affIdx = input.getStringByField("adjidx");
			boltNo = input.getIntegerByField("bolt");

			srctask = input.getIntegerByField("taskid");

			if (localpStreIdx(pivotId) == true) {

				tmpid = gridIdxcnt++;

				srcTaskId[tmpid] = (int) srctask;

				gridAffs.add(new ArrayList<Double>());
				gridAdjIdx.add(new ArrayList<Integer>());

				pivotVecAna(pivotstr, pivotVec, tmpid);
				pivotCoor(coordstr, gridCoors, tmpid);
				// pivotAffs(affRelStr, gridAffs, tmpid);
				pivotAffs(affRelStr, tmpid);
				// adjIdx(affIdx, gridAdjIdx, tmpid);
				adjIdx(affIdx, tmpid);
				gridBolt[tmpid] = boltNo;
				gridPivot[tmpid] = pivotId;
				gpTaskId[tmpid] = groupTaskId(gridCoors, tmpid,
						TopologyMain.winh);
			}

		} else if (streType.compareTo("calCommand") == 0) {

			commandStr = input.getStringByField("command");
			preTaskId = input.getLongByField("taskid");

			preTaskIdx.add(preTaskId);
			if (preTaskIdx.size() < TopologyMain.preBoltNum) {
				return;
			}

			int resnum = 0, i, j;

			if (ts > curtstamp) {

				for (i = 0; i < gridIdxcnt; ++i) {
					for (j = i + 1; j < gridIdxcnt; ++j) {
						if (checkGrids(i, j) == 1) {
							resnum += corBtwAffInGrids(i, j,
									2 - 2 * TopologyMain.thre, collector,
									curtstamp);
							resnum += corBtwPivots(i, j,
									2 - 2 * TopologyMain.thre, collector,
									curtstamp);

						}
					}
				}

				localIdxRenew();
				curtstamp = ts + 1;
				preTaskIdx.clear();

			}
		}
		return;
	}
}
