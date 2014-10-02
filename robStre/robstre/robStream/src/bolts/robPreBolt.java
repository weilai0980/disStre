package bolts;

import java.util.ArrayList;
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

public class robPreBolt extends BaseBasicBolt {

	// .........memory management for sliding windows.................//
	int declrNum = (int) (TopologyMain.nstreBolt / TopologyMain.preBoltNum + 10);
	public double[][] strevec = new double[declrNum][TopologyMain.winSize + 6];
	public double[][] normvec = new double[declrNum][TopologyMain.winSize + 6];

	public int[] streid = new int[TopologyMain.nstreBolt + 10];
	public int streidCnt = 0;

	public int[] vecst = new int[TopologyMain.nstreBolt + 10];
	public int[] veced = new int[TopologyMain.nstreBolt + 10];
	public int queueLen = TopologyMain.winSize + 5;

	public int[] vecflag = new int[TopologyMain.nstreBolt + 10];

	public int iniFlag = 1;

	public double[] curexp = new double[TopologyMain.nstreBolt + 10],
			cursqrsum = new double[TopologyMain.nstreBolt + 10];

	// ...........computation parameter....................//

	public final double disThre = 2 - 2 * TopologyMain.thre;
	public final double cellEps = Math.sqrt(disThre);
	public final double taskEps = cellEps / TopologyMain.cellTask;
	public int shufDim = TopologyMain.winSize - 1;
	public int hSpaceTaskNum;
	public int hSpaceCellNum;

	// ...........emitting streams....................//

	public String ptOutputStr;
	public String vecOutputStr;
	public double taskCoor;
	public int[] preTaskCoor = new int[TopologyMain.nstreBolt + 10];
	public long localTaskId=0;

	// ............input time order..............//

	String streType = new String();
	double ts = 0.0;
	// for long sliding window
	// public double curtstamp = TopologyMain.winSize - 1;
	public double curtstamp = 0.0;
	public double ststamp = 0.0;
	String commandStr=new String(), preCommandStr=new String();

	public int calTaskCoor(int idx, int hspaceTask, int hspaceCell) {
		int k = shufDim;
		int tmp = 0, taskc;

		normvec[idx][k] = (strevec[idx][k] - curexp[idx])
				/ Math.sqrt(cursqrsum[idx] - TopologyMain.winSize * curexp[idx]
						* curexp[idx]);

		if (normvec[idx][k] >= 0) {

			tmp = (int) Math.ceil((double) normvec[idx][k] / cellEps);
		} else {
			tmp = (int) Math.ceil((double) -1 * normvec[idx][k] / cellEps);
		}

		if (tmp == hspaceCell) {
			if (normvec[idx][k] >= 0) {

				taskc = 2 * hspaceTask;

			} else {
				taskc = 1;
			}
		} else {

			if (normvec[idx][k] >= 0) {

				taskc = (int) Math.ceil((double) normvec[idx][k] / taskEps);
				taskc = taskc + hspaceTask;

			} else {
				taskc = (int) Math
						.ceil((double) -1 * normvec[idx][k] / taskEps);
				taskc = hspaceTask - taskc + 1;

			}

		}
		--taskc;

		return taskc;
	}

	public String recentPointUpdate(int idx) {
		if (veced[idx] == 0) {
			return Double.toString(strevec[idx][queueLen - 1]);
		} else {
			return Double.toString(strevec[idx][(veced[idx] - 1) % queueLen]);
		}
	}

	public String vectorUpdate(int idx, int taskcoor, int preTaskcoor) {

		String coorstr = new String();
		int k = vecst[idx];
		while (k != veced[idx]) {

			coorstr = coorstr + Double.toString(strevec[idx][k]) + ",";

			k = (k + 1) % queueLen;
		}

		return coorstr;
	}

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
			oldval = strevec[tmpsn][vecst[tmpsn]];
			newval = val;
			vecst[tmpsn] = (vecst[tmpsn] + 1 * flag) % queueLen;
			if (shufDim == (queueLen - 1)) {
				if (vecst[tmpsn] < shufDim) {
					shufDim = 0;
				}
			} else {
				if (vecst[tmpsn] > shufDim) {
					shufDim = veced[tmpsn];
				}
			}
			veced[tmpsn] = (veced[tmpsn] + 1) % queueLen;

			curexp[tmpsn] = curexp[tmpsn] - oldval / TopologyMain.winSize
					* flag + newval / TopologyMain.winSize;
			cursqrsum[tmpsn] = cursqrsum[tmpsn] - oldval * oldval * flag
					+ newval * newval;

			vecflag[tmpsn] = 1;
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

		for (int j = 0; j < TopologyMain.nstreBolt + 10; j++) {
			vecst[j] = 0;
			veced[j] = 0;

			// for long sliding window
			// veced[j] = TopologyMain.winSize - 1;

			vecflag[j] = 0;
			streid[j] = 0;

			curexp[j] = 0;
			cursqrsum[j] = 0;

			preTaskCoor[j] = -1;
		}
		taskCoor = 0.0;

		hSpaceTaskNum = (int) Math.floor(1.0 / cellEps) * TopologyMain.cellTask
				+ 1;
		hSpaceCellNum = (int) Math.ceil(1.0 / cellEps);

		ptOutputStr = new String();
		vecOutputStr = new String();
		
		localTaskId=context.getThisTaskId();

		return;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

		declarer.declareStream("streamData",new Fields("repType", "strevec", "ts", "taskCoor",
				"streId", "gap"));
		
		declarer.declareStream("calCommand",new Fields("command","taskid"));
		return;
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub

		int i = 0, curTask = 0, tmpgap = 0;
		streType = input.getSourceStreamId();

		if (streType.compareTo("dataStre") == 0) {

			ts = input.getDoubleByField("ts");
			double tmpval = input.getDoubleByField("value");
			int sn = input.getIntegerByField("sn");

			if (Math.abs(ts - curtstamp) <= 1e-3) {

				idxNewTuple(sn, tmpval, 1 - iniFlag);
			}
		} else if (streType.compareTo("contrStre") == 0) {
			
			commandStr= input.getStringByField("command");
			
			if(commandStr.compareTo(preCommandStr)==0)
			{
				return;
			}

			if (ts - ststamp >= TopologyMain.winSize) {

				ststamp++;

				for (i = 0; i < streidCnt; ++i) {
					curTask = calTaskCoor(i, hSpaceTaskNum, hSpaceCellNum);
					tmpgap = Math.abs(curTask - preTaskCoor[i]);

					ptOutputStr = recentPointUpdate(i);

					if (curTask == preTaskCoor[i]) {

						collector.emit("streamData",new Values(0, ptOutputStr, curtstamp,
								curTask, streid[i], 0));
					} else {

						vecOutputStr = vectorUpdate(i, curTask, preTaskCoor[i]);

						if (curTask > preTaskCoor[i]) {
							if (tmpgap >= TopologyMain.cellTask) {
								collector.emit("streamData",new Values(1, vecOutputStr,
										curtstamp, curTask, streid[i], tmpgap));
							} else {
								collector.emit("streamData",new Values(1, vecOutputStr,
										curtstamp, curTask, streid[i], tmpgap));

								collector.emit("streamData",new Values(10, ptOutputStr,
										curtstamp, curTask, streid[i], tmpgap)); // need
																					// skip
							}
						} else {

							if (tmpgap >= TopologyMain.cellTask) {

								collector.emit("streamData",new Values(-1, vecOutputStr,
										curtstamp, curTask, streid[i], tmpgap));
							} else {
								collector.emit("streamData",new Values(-1, vecOutputStr,
										curtstamp, curTask, streid[i], tmpgap));

								collector.emit("streamData",new Values(-10, ptOutputStr,
										curtstamp, curTask, streid[i], tmpgap));
							}
						}
					}
					preTaskCoor[i] = curTask;
				}
				iniFlag = 0;

			}
			
			collector.emit("calCommand",new Values("done"+Double.toString(curtstamp),localTaskId));

			// .....update for next tuple...............//
			preCommandStr=commandStr;
			
			for (int j = 0; j < TopologyMain.nstreBolt + 5; ++j) {

				vecflag[j] = 0;
			}
			curtstamp = ts + 1;

		}
	}
}
