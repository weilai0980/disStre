package bolts;

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

public class llcorrePre extends BaseBasicBolt {

	public int taskId = 0;

	// .........time order....................//

	double curtstamp = TopologyMain.winSize - 1;
	// public double ststamp = -TopologyMain.winSize+1;
	double ststamp = 0.0;
	String streType = new String();
	String commandStr = new String(), preCommandStr = new String();
	double ts = 0.0, retriTs = 0.0;
	// .........memory usage......................//

	int declrNum = (int) (TopologyMain.nstream / TopologyMain.preBoltNum + 1);
	double[][] strevec = new double[declrNum][TopologyMain.winSize + 2];
	double[][] normvec = new double[declrNum][TopologyMain.winSize + 2];

	int[] streid = new int[declrNum + 1];
	int streidCnt = 0;

	int[] vecst = new int[declrNum + 1];
	int[] veced = new int[declrNum + 1];
	int queueLen = TopologyMain.winSize + 1;

	int iniFlag = 1;
	HashSet<Integer> oncePro = new HashSet<Integer>();

	// ...........Computation parameter....................//

	final double disThre = 2 - 2 * TopologyMain.thre;
	final double epsilon = Math.sqrt(disThre);
	int localTaskIdx = 0;

	// ............custom metric............

	double emByte = 0.0, dirCnt = 0;

	// transient CountMetric _contData;

	void iniMetrics(TopologyContext context) {
		// _contData= new CountMetric();
		// context.registerMetric("emByte_count", _contData, 5);
	}

	void updateMetrics(double val, boolean isWin) {
		// _contData.incrBy((long)val);

		return;
	}

	// .....................................

	public double[] curexp = new double[declrNum + 10],
			curdev = new double[declrNum + 10],
			cursqr = new double[declrNum + 10],
			cursum = new double[declrNum + 10];

	public String prepCellVec(int memidx, int partDim) {

		int k = 0, dimcnt = 0;
		String str = new String();
		k = veced[memidx];
		double tmpnorm = 0.0;

		while (k != veced[memidx] && (dimcnt++) < partDim) {

			tmpnorm = (strevec[memidx][k] - curexp[memidx])
					/ Math.sqrt(curdev[memidx]);

			if (tmpnorm >= 0) {

				str = str + (int) Math.floor((double) tmpnorm / epsilon) + ",";
				// it is floor here

			} else {
				str = str + (-1)
						* (int) Math.ceil((double) -1 * tmpnorm / epsilon)
						+ ",";
			}

			k = (k + 1) % queueLen;
		}
		return str;
	}

	public String prepStreVec(int memidx) {

		int k = 0;
		String str = new String();
		k = veced[memidx];
		while (k != veced[memidx]) {

			str = str + Double.toString(strevec[memidx][k]) + ",";

			k = (k + 1) % queueLen;
		}
		return str;
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

		strevec[tmpsn][veced[tmpsn]] = val;
		veced[tmpsn] = (veced[tmpsn] + 1) % queueLen;

		oldval = strevec[tmpsn][vecst[tmpsn]];
		newval = val;

		vecst[tmpsn] = (vecst[tmpsn] + 1 * flag) % queueLen;

		curexp[tmpsn] = curexp[tmpsn] - oldval / TopologyMain.winSize * flag
				+ newval / TopologyMain.winSize;
		cursqr[tmpsn] = cursqr[tmpsn] - oldval * oldval * flag + newval
				* newval;
		cursum[tmpsn] = cursum[tmpsn] - oldval * flag + newval;

		curdev[tmpsn] = cursqr[tmpsn] + TopologyMain.winSize * curexp[tmpsn]
				* curexp[tmpsn] - 2 * cursum[tmpsn] * curexp[tmpsn];

	}

	@Override
	public void cleanup() {

	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		// TODO Auto-generated method stub

		taskId = context.getThisTaskId();
		localTaskIdx = context.getThisTaskIndex();

		for (int j = 0; j < declrNum + 1; j++) {
			vecst[j] = 0;
			veced[j] = 0;

			// for quick sliding window loading
			if (TopologyMain.iniWindow == 0) {
				veced[j] = TopologyMain.winSize - 1;
			}

			streid[j] = 0;

			curexp[j] = 0;
			curdev[j] = 0;
			cursqr[j] = 0;
			cursum[j] = 0;
		}

		iniMetrics(context);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

		declarer.declareStream("dataTup", new Fields("id", "strevec",
				"cellvec", "ts"));

		declarer.declareStream("calCommand", new Fields("command", "taskid"));

		return;
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub

		int i = 0, tmppivot = 0;

		streType = input.getSourceStreamId();

		if (streType.compareTo("dataStre") == 0) {

			ts = input.getDoubleByField("ts");
			double tmpval = input.getDoubleByField("value");
			int sn = input.getIntegerByField("sn");

			if (oncePro.contains(sn) == false) {

				idxNewTuple(sn, tmpval, 1 - iniFlag);
				oncePro.add(sn);
			}

		} else if (streType.compareTo("contrStre") == 0) {

			commandStr = input.getStringByField("command");

			if (commandStr.compareTo(preCommandStr) == 0) {
				return;
			}

			if (ts - ststamp >= TopologyMain.winSize - 1) {

				ststamp++;
				// emByte = 0.0; // for window metric
				dirCnt = 0.0;

				for (i = 0; i < streidCnt; ++i) {

					collector.emit("interStre", new Values(streid[tmppivot],
							prepStreVec(i), prepCellVec(i, TopologyMain.winh),
							curtstamp)); // modification
					iniFlag = 0;

					// .......comm byte metric........
					// emByte += (adjList.get(i).size() * 3 + 2 *
					// TopologyMain.winSize);

					// ...............................

				}
				collector
						.emit("calCommand",
								new Values("done" + Double.toString(curtstamp),
										taskId));

				// .......... custom metrics........
				updateMetrics(emByte, true);

			}

			// .....update for next tuple...............//
			oncePro.clear();

			preCommandStr = commandStr;
			curtstamp = ts + 1;

		}
	}
}
