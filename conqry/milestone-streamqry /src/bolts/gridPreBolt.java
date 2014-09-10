package bolts;

import main.*;

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

	public int[] streid = new int[TopologyMain.nstreBolt + 10];
	public int streidCnt = 0;

	public int iniFlag = 1;
	public int[] direcVec = { -1, 0, 1 };

	public double disThre = 2 - 2 * TopologyMain.thre;

	public double[] curexp = new double[TopologyMain.nstreBolt + 10],
			curdev = new double[TopologyMain.nstreBolt + 10],
			cursqr = new double[TopologyMain.nstreBolt + 10],
			cursum = new double[TopologyMain.nstreBolt + 10];

	public void idxNewTuple(int strid, double val, int flag) {
		int i = 0, tmpsn = 0;
		double oldval = 0.0, newval = 0.0;

		for (i = 0; i < streidCnt; ++i) {
			if (streid[i] == strid) {
				tmpsn = i;
				break;
			}
		}
		if (i == streidCnt) {
			streid[i] = strid;
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

	public int sign = 1;

	public void gridEmit(String hoststr, int coord[],
			BasicOutputCollector collector, int curcor, double ts, int hostid,
			int orgiCoord[]) {

		if (curcor == TopologyMain.winSize) {
			String coordstr = new String();

			coordstr = "";
			for (int i = 0; i < TopologyMain.winSize; ++i) {
				coordstr = coordstr + Integer.toString(coord[i]) + ",";
			}

			collector.emit(new Values(coordstr, hoststr, ts, hostid));
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
			gridEmit(hoststr, coord, collector, curcor + 1, ts, hostid,
					orgiCoord);
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
		declarer.declare(new Fields("coord", "vec", "ts", "hostid"));
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

		int emitflag = 0;

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
					while (k != veced[j]) {

						// * TopologyMain.winSize
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

						k = (k + 1) % queueLen;
					}
					if (emitflag == 1) {
						// System.out.printf("normalized data %s\n",vecstr);
						// System.out.printf("edge lenght of cell: %f\n",Math.sqrt(disThre));

						sign = 1;

						// for(int l=0;l<vecidx;++l)
						// {
						// System.out.printf("%d   ",coord[l]);
						//
						// }
						// System.out.printf("\n");

//						if (streid[j] == 0) {
//							
//							System.out.printf("stream 0 at timestamp %f:  ", curtstamp);
//							for (int l = 0; l < vecidx; ++l) {
//								System.out.printf("   %d   ", coord[l]);
//							}
//							System.out.printf("\n");
//						}
//						
//						if (streid[j] == 1) {
//							
//							System.out.printf("stream 1 at timestamp %f:  ", curtstamp);
//							for (int l = 0; l < vecidx; ++l) {
//								System.out.printf("   %d   ", coord[l]);
//							}
//							System.out.printf("\n");
//						}

						gridEmit(vecstr, norcoord, collector, 0, curtstamp,
								tmpstrid, coord);
					}
				}
			}
			curtstamp = ts;

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
