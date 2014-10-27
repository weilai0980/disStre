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

public class dftPreBolt extends BaseBasicBolt {

	// .........memory management for sliding windows.................//
	int declrNum = (int) (TopologyMain.nstreBolt / TopologyMain.preBoltNum + 10);
	public double[][] strevec = new double[declrNum][TopologyMain.winSize + 6];
	public double[][] normvec = new double[declrNum][TopologyMain.winSize + 6];
	public double[][] dft = new double[declrNum][TopologyMain.dftN * 2 + 2];
	public int[][] dftCell = new int[declrNum][TopologyMain.dftN * 2 + 2];

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
	public int hSpaceTaskNum;
	public int hSpaceCellNum;

	// ...........emitting streams....................//

	public String ptOutputStr;
	public String vecOutputStr;
	public double taskCoor;
	public long localTaskId = 0;
	int[] direcVec = { -1, 0, 1 };
	int[] cellCal = new int[TopologyMain.dftN * 2 + 2];

	// ............input time order..............//

	String streType = new String();
	double ts = 0.0;
	// for long sliding window
	// public double curtstamp = TopologyMain.winSize - 1;
	public double curtstamp = 0.0;
	public double ststamp = 0.0;
	String commandStr = new String(), preCommandStr = new String();

	// ..........................................//

	public void calCellCoor(int memidx, int dftNum, int curCnt, int cell[],
			BasicOutputCollector collector, String strevec, String hostCoor,
			double ts) {

		if (curCnt >= dftNum) {
			String cellCoor = new String();
			for (int i = 0; i < dftNum; ++i) {
				cellCoor = cellCoor + Integer.toString(cell[i]) + ",";
			}

			if (cellCoor.compareTo(hostCoor) == 0) {
				collector.emit("streamData", new Values(ts, streid[memidx],
						cellCoor, strevec, 1)); // hostflag=1
			} else {
				collector.emit("streamData", new Values(ts, streid[memidx],
						cellCoor, strevec, 0));
			}

			return;

		}
		for (int i = 0; i < 3; ++i) {

			cell[curCnt] = dftCell[memidx][curCnt] + direcVec[i];

			calCellCoor(memidx, dftNum, curCnt + 1, cell, collector, strevec,
					hostCoor, ts);

		}

		return;
	}

	public String streamVecPrep(int idx) {

		String coorstr = new String();
		int k = vecst[idx];
		while (k != veced[idx]) {

			coorstr = coorstr + Double.toString(strevec[idx][k]) + ",";

			k = (k + 1) % queueLen;
		}

		return coorstr;
	}

	public String dftCellVecPrep(int idx) {

		String coorstr = new String();

		for (int i = 0; i < TopologyMain.dftN * 2; ++i) {
			coorstr = coorstr + Double.toString(dftCell[idx][i]) + ",";
		}
		return coorstr;
	}

	public double complexAng(double real, double img) {
		return Math.atan2(img, real);
	}

	public void dftUpdate(int memidx, double oldval, double newval) {

		int cnt = 0;
		double delAng = 0.0, oldAng = 0.0, ang = 0.0, r = 0.0;

		for (int i = 0; i < TopologyMain.dftN; ++i) {

			oldAng = complexAng(dft[memidx][cnt], dft[memidx][cnt + 1]);
			delAng = 2 * Math.PI * (i + 1) / TopologyMain.winSize;

			ang = complexAng(dft[memidx][cnt], dft[memidx][cnt + 1]) + delAng;
			r = Math.sqrt(dft[memidx][cnt] * dft[memidx][cnt]
					+ dft[memidx][cnt + 1] * dft[memidx][cnt + 1]);

			dft[memidx][cnt] = dft[memidx][cnt] * Math.cos(ang)
					/ Math.cos(oldAng);
			dft[memidx][cnt + 1] = dft[memidx][cnt + 1] * Math.sin(ang)
					/ Math.sin(oldAng);

			r = (newval - oldval) / Math.sqrt(TopologyMain.winSize);

			dft[memidx][cnt] += r * Math.cos(delAng);
			dft[memidx][cnt + 1] += r * Math.sin(delAng);

			dftCell[memidx][cnt] = dftCellCal(dft[memidx][cnt]);
			dftCell[memidx][cnt + 1] = dftCellCal(dft[memidx][cnt + 1]);

			cnt += 2;
		}

		return;
	}

	public int dftCellCal(double val) {
		return val >= 0 ? (int) Math.floor((double) val / cellEps) : -1
				* (int) Math.ceil(-1.0 * val / cellEps);
	}

	public void dftIni(int memidx) {

		int k = vecst[memidx], cnt = 0;
		double ang = 0.0, r = 0.0;

		for (int i = 0; i < TopologyMain.dftN; ++i) {
			while (k != veced[memidx]) {

				r = normvec[memidx][k];
				ang = (double) (-2 * Math.PI * i * k / TopologyMain.winSize);

				dft[memidx][cnt] += r * Math.cos(ang); // real
				dft[memidx][cnt + 1] += r * Math.sin(ang); // imaginary

				k = (k + 1) % queueLen;
			}

			dftCell[memidx][cnt] = dftCellCal(dft[memidx][cnt]);
			dftCell[memidx][cnt + 1] = dftCellCal(dft[memidx][cnt + 1]);

			cnt += 2;
		}

		return;
	}

	public void streNorm(int memidx) {
		int k = vecst[memidx];
		while (k != veced[memidx]) {

			normvec[memidx][k] = (strevec[memidx][k] - curexp[memidx])
					/ Math.sqrt(cursqrsum[memidx] - TopologyMain.winSize
							* curexp[memidx] * curexp[memidx]);

			k = (k + 1) % queueLen;
		}

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

			veced[tmpsn] = (veced[tmpsn] + 1) % queueLen;

			curexp[tmpsn] = curexp[tmpsn] - oldval / TopologyMain.winSize
					* flag + newval / TopologyMain.winSize;
			cursqrsum[tmpsn] = cursqrsum[tmpsn] - oldval * oldval * flag
					+ newval * newval;

			vecflag[tmpsn] = 1;

			streNorm(tmpsn);
			if (flag == 1) {
				dftUpdate(tmpsn, oldval, newval);
			}
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

			// preTaskCoor[j] = -1;
		}
		taskCoor = 0.0;

		hSpaceTaskNum = (int) Math.floor(1.0 / cellEps) * TopologyMain.cellTask
				+ 1;
		hSpaceCellNum = (int) Math.ceil(1.0 / cellEps);

		ptOutputStr = new String();
		vecOutputStr = new String();

		localTaskId = context.getThisTaskId();

		return;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

		declarer.declareStream("streamData", new Fields("ts", "streId",
				"cellCoor", "strevec","dftvec", "hostFlag"));

		declarer.declareStream("calCommand", new Fields("command", "taskid"));
		return;
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub

		int i = 0;
		streType = input.getSourceStreamId();

		if (streType.compareTo("dataStre") == 0) {

			ts = input.getDoubleByField("ts");
			double tmpval = input.getDoubleByField("value");
			int sn = input.getIntegerByField("sn");

			if (Math.abs(ts - curtstamp) <= 1e-3) {

				idxNewTuple(sn, tmpval, 1 - iniFlag);
			}
		} else if (streType.compareTo("contrStre") == 0) {

			commandStr = input.getStringByField("command");

			if (commandStr.compareTo(preCommandStr) == 0) {
				return;
			}

			if (ts - ststamp >= TopologyMain.winSize - 1) {

				ststamp++;

				if (iniFlag == 1) {
					for (i = 0; i < streidCnt; ++i) {

						dftIni(i);
					}
				}

				for (i = 0; i < streidCnt; ++i) {
					calCellCoor(i, TopologyMain.dftN * 2, 0, cellCal,
							collector, streamVecPrep(i), dftCellVecPrep(i), ts);
				}

				iniFlag = 0;

			}

			collector
					.emit("calCommand",
							new Values("done" + Double.toString(curtstamp),
									localTaskId));

			// .....status update for the next tuple...............//
			preCommandStr = commandStr;

			for (int j = 0; j < TopologyMain.nstreBolt + 5; ++j) {

				vecflag[j] = 0;
			}
			curtstamp = ts + 1;

		}
	}
}
