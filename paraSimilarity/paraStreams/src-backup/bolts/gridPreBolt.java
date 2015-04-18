package bolts;

import main.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class gridPreBolt extends BaseBasicBolt {

	public static int gtaskId = 0;
	public int taskId = 0;

	public double curtstamp = 0.0;
	public double ststamp = 0.0;

	public double[][] strevec = new double[TopologyMain.nstreBolt + 10][TopologyMain.winSize + 10];
	public double[][] normvec = new double[TopologyMain.nstreBolt + 10][TopologyMain.winSize + 10];

	public int[] vecst = new int[TopologyMain.nstreBolt + 10];
	public int[] veced = new int[TopologyMain.nstreBolt + 10];
	public int queueLen = TopologyMain.winSize + 10;

	public int[] vecflag = new int[TopologyMain.nstreBolt + 10];

	HashMap<Integer, Integer> streidMap = new HashMap<Integer, Integer>();
	public int[] streid = new int[TopologyMain.nstreBolt + 10];
	public int streidCnt = 0;

	public int iniFlag = 1;
	public int[] direcVec = { -1, 0, 1 };

	public double disThre = 2 - 2 * TopologyMain.thre;

	public double[] curexp = new double[TopologyMain.nstreBolt + 10],
			curdev = new double[TopologyMain.nstreBolt + 10],
			cursqr = new double[TopologyMain.nstreBolt + 10],
			cursum = new double[TopologyMain.nstreBolt + 10];

	ArrayList<String> adjCell = new ArrayList<String>();

	public double boundCell;

	public void idxNewTuple(int strid, double val, int flag) {
		int tmpsn = 0;
		double oldval = 0.0, newval = 0.0;

		if (streidMap.containsKey(strid) == true) {
			tmpsn = streidMap.get(strid);
		} else {
			streidMap.put(strid, streidCnt);
			streid[streidCnt] = strid;
			tmpsn = streidCnt;
			streidCnt++;
		}

		if (vecflag[tmpsn] == 0) {

			strevec[tmpsn][veced[tmpsn]] = val;
			veced[tmpsn] = (veced[tmpsn] + 1) % queueLen;

			oldval = strevec[tmpsn][vecst[tmpsn]];
			newval = val;

			vecst[tmpsn] = (vecst[tmpsn] + 1 * flag) % queueLen;

			curexp[tmpsn] = curexp[tmpsn] - oldval / TopologyMain.winSize
					* flag + newval / TopologyMain.winSize;
			cursqr[tmpsn] = cursqr[tmpsn] - oldval * oldval * flag + newval
					* newval;
			cursum[tmpsn] = cursum[tmpsn] - oldval * flag + newval;

			curdev[tmpsn] = cursqr[tmpsn] + TopologyMain.winSize
					* curexp[tmpsn] * curexp[tmpsn] - 2 * cursum[tmpsn]
					* curexp[tmpsn];

			vecflag[tmpsn] = 1;

		}
	}

	// public int sign = 1;

	// public void gridEmit(String hoststr, int coord[],
	// BasicOutputCollector collector, int curcor, double ts, int hostid,
	// int orgiCoord[]) {
	//
	// if (curcor == TopologyMain.winSize) {
	// String coordstr = new String();
	//
	// coordstr = "";
	// for (int i = 0; i < TopologyMain.winSize; ++i) {
	// coordstr = coordstr + Integer.toString(coord[i]) + ",";
	// }
	//
	// collector.emit(new Values(coordstr, hoststr, ts, hostid));
	// return;
	// }
	// int org = orgiCoord[curcor];
	// for (int i = 0; i < 3; ++i) {
	// coord[curcor] = org + direcVec[i];
	// if (coord[curcor] == 0) {
	// if (i == 0) {
	// coord[curcor] = -1;
	// } else if (i == 2) {
	// coord[curcor] = 1;
	// }
	// }
	// gridEmit(hoststr, coord, collector, curcor + 1, ts, hostid,
	// orgiCoord);
	// }
	// }

	public ArrayList<Integer> emitStack = new ArrayList<Integer>();

	public void gridEmitNoRecur(int orgiCoord[]) {

		int curlay = 0;
		int[] tmpCoor = new int[TopologyMain.winSize + 5];
		String coordstr = new String();

		int stkSize = 0, curdir = 0, i = 0, tmpdir = 0;

		// .....ini....................//
		emitStack.add(curdir);
		stkSize++;
		tmpdir = curdir;

		tmpCoor[curlay] = orgiCoord[curlay] + direcVec[tmpdir];
		if (tmpCoor[curlay] == 0) {
			if (tmpdir == 0) {
				tmpCoor[curlay] = -1;
			} else if (tmpdir == 2) {
				tmpCoor[curlay] = 1;
			}
		}

		curlay++;
		curdir = -1;
		// ...........................//


		int popflag=0;
		while (emitStack.size() != 0) {
			
			if(popflag==1)
			{
				curdir = emitStack.get(stkSize - 1);
				emitStack.remove(stkSize - 1);
				stkSize--;
				curlay--;
				
				popflag=0;
			}
			
			if (curlay >= TopologyMain.winSize) {

				coordstr = "";
				for (i = 0; i < TopologyMain.winSize; ++i) {
					coordstr = coordstr + Integer.toString(tmpCoor[i]) + ",";

					if (tmpCoor[i] > boundCell)
						break;
					if (tmpCoor[i] < -1 * boundCell)
						break;

				}

				if (i >= TopologyMain.winSize) {
					adjCell.add(coordstr);
				}

//				curdir = emitStack.get(stkSize - 1);
//				emitStack.remove(stkSize - 1);
//				stkSize--;
//				curlay--;
				
				popflag=1;

			} else {

				if (curdir + 1 > 2) {

//					curdir = emitStack.get(stkSize - 1);
//					emitStack.remove(stkSize - 1);
//					stkSize--;
//					curlay--;
					
					popflag=1;

					continue;
				} else {

					emitStack.add(curdir + 1);
					stkSize++;

					tmpdir = curdir + 1;
					tmpCoor[curlay] = orgiCoord[curlay] + direcVec[tmpdir];
					if (tmpCoor[curlay] == 0) {
						if (tmpdir == 0) {
							tmpCoor[curlay] = -1;
						} else if (tmpdir == 2) {
							tmpCoor[curlay] = 1;
						}
					}

					curlay++;
					curdir = -1;

				}
			}
		}

		return;
	}

	public void gridEmit(String hoststr, int coord[], int curcor, double ts,
			int hostid, int orgiCoord[]) {

		if (curcor == TopologyMain.winSize) {
			String coordstr = new String();

			coordstr = "";
			for (int i = 0; i < TopologyMain.winSize; ++i) {
				coordstr = coordstr + Integer.toString(coord[i]) + ",";

				if (coord[i] > boundCell)
					return;
				if (coord[i] < -1 * boundCell)
					return;

			}

			adjCell.add(coordstr);
			// collector.emit(new Values(coordstr, hoststr, ts, hostid));
			return;
		}
		int org = orgiCoord[curcor];
		for (int i = 0; i < 3; ++i) {
			coord[curcor] = org + direcVec[i];
			if (coord[curcor] == 0) {
				if (i == 0) {
					coord[curcor] = -1;
				} else if (i == 2) {
					coord[curcor] = 1;
				}
			}
			gridEmit(hoststr, coord, curcor + 1, ts, hostid, orgiCoord);
		}
	}

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

		taskId = gtaskId;
		gtaskId++;

		boundCell = Math.ceil(1 / Math.sqrt(disThre));

		for (int j = 0; j < TopologyMain.nstreBolt + 10; j++) {
			vecst[j] = 0;
			veced[j] = 0;

			vecflag[j] = 0;
			streid[j] = 0;

			curexp[j] = 0;
			curdev[j] = 0;
			cursqr[j] = 0;
			cursum[j] = 0;
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("coord", "vec", "ts", "hostid", "lflag"));
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub

		double ts = input.getDoubleByField("ts");
		double tmpval = input.getDoubleByField("value");
		int sn = input.getIntegerByField("sn");
		int vecidx = 0;

		int[] coord = new int[TopologyMain.winSize + 10], norcoord = new int[TopologyMain.winSize + 10];
		int tmpstrid = 0, k = 0, j = 0;
		String vecstr = new String();
		String cellvec = new String();

		int emitflag = 0;
		
		int pos=1,neg=0;

		if (ts > curtstamp) {

			if (ts - ststamp >= TopologyMain.winSize) {

				ststamp++;
				iniFlag = 0;

				for (j = 0; j < streidCnt; ++j) {
					tmpstrid = streid[j];

					vecstr = "";
					vecidx = 0;

					emitflag = 1;

					k = vecst[j];
					
					
					cellvec="";
					while (k != veced[j]) {

						if (Math.abs(curdev[j] - 0.0) < 1e-6) {
							normvec[j][k] = 0.0;
							emitflag = 0;
							break;
						} else {
							normvec[j][k] = (strevec[j][k] - curexp[j])
									/ Math.sqrt(curdev[j]);
						}

						vecstr = vecstr + Double.toString(normvec[j][k]) + ",";

						if (normvec[j][k] >= 0) {
							coord[vecidx++] = (int) Math
									.ceil((double) normvec[j][k]
											/ Math.sqrt(disThre));
						} else {
							coord[vecidx++] = -1
									* (int) Math.ceil((double) -1
											* normvec[j][k]
											/ Math.sqrt(disThre));

						}

						cellvec = cellvec + Integer.toString(coord[vecidx - 1])
								+ ",";
						
//						coordstr = coordstr + Integer.toString(coord[i]) + ",";

						k = (k + 1) % queueLen;
					}
					if (emitflag == 1) {

						adjCell.clear();

						emitStack.clear();
						gridEmitNoRecur(coord);

						// gridEmit(vecstr, norcoord, 0, curtstamp, tmpstrid,
						// coord);

						for (String tmpstr : adjCell) {
							
							if (tmpstr.compareTo(cellvec) == 0) {
								collector.emit(new Values(tmpstr, vecstr,
										curtstamp, tmpstrid, pos));
							} else {
								collector.emit(new Values(tmpstr, vecstr,
										curtstamp, tmpstrid, neg));
							}

						}

//						collector.emit(new Values(cellvec, vecstr,
//								curtstamp, tmpstrid, pos));
						
						
						
						// declarer.declare(new Fields("coord", "vec", "ts",
						// "hostid"));
					}
				}
			}
			curtstamp = ts;

			streidMap.clear();
			streidCnt = 0;
			for (j = 0; j < TopologyMain.nstreBolt + 5; ++j) {
				vecflag[j] = 0;
			}
			idxNewTuple(sn, tmpval, 1 - iniFlag);

		} else if (ts < curtstamp) {
			System.out.printf("!!!!!!!!!!!!! time sequence disorder\n");
		} else if (Math.abs(ts - curtstamp) <= 1e-3) {

			idxNewTuple(sn, tmpval, 1 - iniFlag);
		}
	}
}
