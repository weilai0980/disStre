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

	public static int gtaskId = 0;
	public int taskId = 0;

	public double curtstamp = TopologyMain.winSize - 1;
	// public double ststamp = -TopologyMain.winSize+1;
	public double ststamp = 0.0;

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

	public double disThre = 2 - 2 * TopologyMain.thre;

	public double cellEps = Math.sqrt(disThre);
	public double taskEps = 1 / cellEps / TopologyMain.cellTask;
	public double taskCoor;

	public int shufDim = 1;
	public int[] preTaskCoor = new int[TopologyMain.nstreBolt + 10];
	public int hSpaceTaskNum;

	public String ptOutputStr;
	public String vecOutputStr;

	public double[] curexp = new double[TopologyMain.nstreBolt + 10],
			curdev = new double[TopologyMain.nstreBolt + 10],
			cursqr = new double[TopologyMain.nstreBolt + 10],
			cursum = new double[TopologyMain.nstreBolt + 10];

	public int calTaskCoor(int idx, int hspaceTask) {
		int k = 0;
		k = (shufDim + 1) % queueLen;
		int tmp = 0, taskc;

		if (normvec[idx][k] >= 0) {

			tmp = (int) Math.ceil((double) normvec[idx][k] / cellEps);
		} else {
			tmp = -1 * (int) Math.ceil((double) -1 * normvec[idx][k] / cellEps);
		}

		if (normvec[idx][k] >= 0) {

			taskc = (int) Math.ceil((double) (normvec[idx][k] - tmp * cellEps)
					/ taskEps);
			
			taskc=taskc + hspaceTask;

		} else {
			taskc = -1
					* (int) Math.ceil((double) (-1 * normvec[idx][k] - tmp
							* cellEps)
							/ taskEps);
		}

		return taskc;
	}

	public String recentPointUpdate(int idx) {
		if (veced[idx] == 0) {
			return Double.toString(strevec[idx][queueLen - 1]);
		} else {
			return Double.toString(strevec[idx][(veced[idx] - 1) % queueLen]);
		}
	}

	public String AdaptiveUpdate(int idx, int taskcoor, int preTaskcoor) {

		String coorstr = new String();
		int k = 0;
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

	/**
	 * At the end of the spout (when the cluster is shutdown We will show the
	 * word counters
	 */
	@Override
	public void cleanup() {
		// System.out.println("-- Word Counter [" + name + "-" + id + "] --");
		// for (Map.Entry<String, Integer> entry : counters.entrySet()) {
		// System.out.println(entry.getKey() + ": " + entry.getValue());
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		// TODO Auto-generated method stub

		taskId = gtaskId;
		gtaskId++;
		taskId = context.getThisTaskId();

		for (int j = 0; j < TopologyMain.nstreBolt + 10; j++) {
			vecst[j] = 0;
			veced[j] = 0;
			veced[j] = TopologyMain.winSize - 1;

			vecflag[j] = 0;
			streid[j] = 0;

			curexp[j] = 0;
			curdev[j] = 0;
			cursqr[j] = 0;
			cursum[j] = 0;
		}
		taskCoor = 0.0;

		hSpaceTaskNum = (int) Math.floor(1.0 / cellEps) * TopologyMain.cellTask
				+ 1;

		ptOutputStr = new String();
		vecOutputStr = new String();

		return;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

		declarer.declare(new Fields("repType", "strevec", "ts", "taskCoor",
				"streId", "gap"));
		return;
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub

		double ts = input.getDoubleByField("ts");
		double tmpval = input.getDoubleByField("value");
		int sn = input.getIntegerByField("sn");
		int i = 0, curTask = 0, tmpgap = 0;

		if (ts > curtstamp) {

			if (ts - ststamp >= TopologyMain.winSize) {

				ststamp++;

				for (i = 0; i < streidCnt; ++i) {
					curTask = calTaskCoor(i, hSpaceTaskNum);
					tmpgap = Math.abs(curTask - preTaskCoor[i]);

					ptOutputStr = recentPointUpdate(i);

					if (curTask == preTaskCoor[i]) {

						collector.emit(new Values(0, ptOutputStr, curtstamp,
								curTask, streid[i], 0));
					} else {

						vecOutputStr = AdaptiveUpdate(i, curTask,
								preTaskCoor[i]);

						if (curTask > preTaskCoor[i])

						{
							collector.emit(new Values(1, vecOutputStr,
									curtstamp, curTask, streid[i], tmpgap));

							collector.emit(new Values(10, ptOutputStr,
									curtstamp, curTask, streid[i], tmpgap)); // need
																				// skip
						}

						else {

							collector.emit(new Values(-1, vecOutputStr,
									curtstamp, curTask, streid[i], tmpgap));

							collector.emit(new Values(-10, ptOutputStr,
									curtstamp, curTask, streid[i], tmpgap));
						}
					}
					preTaskCoor[i] = curTask;
				}
				iniFlag = 0;
			}

			// .....update for next tuple...............//
			curtstamp = ts;

			for (int j = 0; j < TopologyMain.nstreBolt + 5; ++j) {

				vecflag[j] = 0;
			}
			idxNewTuple(sn, tmpval, 1 - iniFlag);

		} else if (ts < curtstamp) {
			System.out
					.printf("!!!!!!!!!!!!! AdjustPreBolt time sequence disorder\n");
		} else if (Math.abs(ts - curtstamp) <= 1e-3) {

			idxNewTuple(sn, tmpval, 1 - iniFlag);
		}

	}

}
