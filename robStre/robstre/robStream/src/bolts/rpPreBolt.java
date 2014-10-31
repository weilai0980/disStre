package bolts;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
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

public class rpPreBolt extends BaseBasicBolt {

	// .........memory management for sliding windows.................//
	int declrNum = (int) (TopologyMain.nstreBolt / TopologyMain.preBoltNum + 10);
	public double[][] strevec = new double[declrNum][TopologyMain.winSize + 6];
	public double[][] normvec = new double[declrNum][TopologyMain.winSize + 6];

	public double[][][] rpvec = new double[declrNum][TopologyMain.rp_vecnum][TopologyMain.rp_dimnum + 1];

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

	double[][][] rpMat = new double[TopologyMain.rp_vecnum | +1][TopologyMain.rp_dimnum + 1][TopologyMain.winSize + 1];

	// ...........emitting streams....................//

	public long localTaskId = 0;

	// ............input time order..............//

	String streType = new String();
	double ts = 0.0;
	// for long sliding window
	// public double curtstamp = TopologyMain.winSize - 1;
	public double curtstamp = 0.0;
	public double ststamp = 0.0;
	String commandStr = new String(), preCommandStr = new String();

	// ..........................................//

	public String streamVecPrep(int idx) {

		String coorstr = new String();
		int k = vecst[idx];
		while (k != veced[idx]) {

			coorstr = coorstr + Double.toString(strevec[idx][k]) + ",";

			k = (k + 1) % queueLen;
		}

		return coorstr;
	}

	public String normStreamVecPrep(int idx) {

		String coorstr = new String();
		int k = vecst[idx];
		while (k != veced[idx]) {

			coorstr = coorstr + Double.toString(normvec[idx][k]) + ",";

			k = (k + 1) % queueLen;
		}

		return coorstr;
	}

	public double complexAng(double real, double img) {
		return Math.atan2(img, real);
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

		}
	}

	void rpCal(int idx) {

		double tmpDot = 0.0;

		for (int i = 0; i < TopologyMain.rp_vecnum; ++i) {

			for (int j = 0; j < TopologyMain.rp_dimnum; ++j) {

				tmpDot = 0.0;

				for (int k = 0; k < TopologyMain.winSize; ++k) {

					tmpDot = tmpDot + normvec[idx][k] * rpMat[i][j][k];

				}
				rpvec[idx][i][j] = tmpDot > 0 ? 1 : 0;

			}
		}

		return;
	}

	public String rpBucketPrep(int idx, int bucketCnt) {

		String str = new String();

		for (int i = 0; i < TopologyMain.rp_dimnum; ++i) {
			str = str + rpvec[idx][bucketCnt][i] + ",";

		}

		return str;

	}

	// ............random project.............//
	public void readProjectMatrix() throws FileNotFoundException {
		FileReader fstream = new FileReader(TopologyMain.rp_matFile);
		BufferedReader reader = new BufferedReader(fstream);

		String line;
		int len = 0, pre = 0, dimcnt = 0, cnt = 0, veccnt = 0;
		try {

			while ((line = reader.readLine()) != null) {

				len = line.length();
				pre = 0;
				cnt = 0;
				dimcnt++;

				if ((int) (dimcnt / TopologyMain.rp_dimnum) == 1) {
					veccnt++;
					dimcnt = 0;
				}

				for (int i = 0; i < len; i++) {
					if (line.charAt(i) == ',') {
						rpMat[veccnt][dimcnt][cnt++] = Double.parseDouble(line
								.substring(pre, i));
						pre = i + 1;
					}
				}
			}

			reader.close();
			fstream.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	// ......................................//

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

		}


		hSpaceTaskNum = (int) Math.floor(1.0 / cellEps) * TopologyMain.cellTask
				+ 1;
		hSpaceCellNum = (int) Math.ceil(1.0 / cellEps);


		localTaskId = context.getThisTaskId();

		try {
			readProjectMatrix();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

		declarer.declareStream("streamData", new Fields("ts", "streId",
				"bucket", "strevec"));

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

				for (i = 0; i < streidCnt; ++i) {

					rpCal(i);

					for (int j = 0; j < TopologyMain.rp_vecnum; ++j) {

						collector.emit("streamData", new Values(curtstamp,
								streid[i], rpBucketPrep(i, j),
								normStreamVecPrep(i))); // modification

						// declarer.declareStream("streamData", new Fields("ts",
						// "streId",
						// "bucket", "strevec"));

					}

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
