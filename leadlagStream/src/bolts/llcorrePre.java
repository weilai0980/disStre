package bolts;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
	int[][] cellvec = new int[declrNum][TopologyMain.winSize + 2];

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

	ArrayList<Integer> emitStack = new ArrayList<Integer>();
	List<Integer> taskSet = new LinkedList<Integer>();

	int[] direcVec = { -1, 0, 1 };

	int taskCnt = TopologyMain.calBoltNum;

	List<Integer> tasks = new LinkedList<Integer>();

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

		int k = 0, dimcnt = 0, tmpdim = 0;
		String str = new String();
		k = vecst[memidx];
		double tmpnorm = 0.0;

		while (k != veced[memidx] && (dimcnt++) < partDim) {

			tmpnorm = (strevec[memidx][k] - curexp[memidx])
					/ Math.sqrt(curdev[memidx]);

			normvec[memidx][k] = tmpnorm;

			if (tmpnorm >= 0) {

				tmpdim = (int) Math.floor((double) tmpnorm / epsilon);
				str = str + Integer.toString(tmpdim) + ",";
				// it is floor here

			} else {
				tmpdim = (-1)
						* (int) Math.ceil((double) -1 * tmpnorm / epsilon);
				str = str + Integer.toString(tmpdim) + ",";
			}

			cellvec[memidx][dimcnt - 1] = tmpdim;

			k = (k + 1) % queueLen;
		}
		return str;
	}

	public String prepStreVec(int memidx) { // interface for dimension reduction

		int k = 0;
		String str = new String();
		k = vecst[memidx];
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

	public int[] posDirecVec = { 0, 1 };
	public int[] negDirecVec = { 0, -1 };

	public void broadcastEmitNoRecurSubDim2Part(int orgiCoord[], int dimSign[],
			int dimSignBound[]) {

		int curlay = 0;
		int[] tmpCoor = new int[TopologyMain.winSize + 5];

		int stkSize = 0, curdir = -1, tmpdir = 0, emittask = 0;
		double tmptaskId = 0.0;

		// .....ini....................//
		if (dimSign[curlay] == 0) {
			tmpCoor[curlay] = orgiCoord[curlay];

			emitStack.add(0);
			stkSize++;
		} else if (dimSign[curlay] == 1) {

			tmpdir = curdir + 1;
			tmpCoor[curlay] = orgiCoord[curlay] + posDirecVec[tmpdir];

			emitStack.add(0);
			stkSize++;
		} else if (dimSign[curlay] == -1) {

			tmpdir = curdir + 1;
			tmpCoor[curlay] = orgiCoord[curlay] + negDirecVec[tmpdir];

			emitStack.add(0);
			stkSize++;
		}

		curlay++;
		curdir = -1;
		// ...........................//

		int popflag = 0;

		while (emitStack.size() != 0) {

			if (popflag == 1) {
				curdir = emitStack.get(stkSize - 1);
				emitStack.remove(stkSize - 1);
				stkSize--;
				curlay--;

				popflag = 0;
			}

			if (curlay >= TopologyMain.winh) {

				tmptaskId = tmpCoor[0];
				for (int j = 1; j < TopologyMain.winh; ++j) {
					tmptaskId = tmptaskId + (tmpCoor[j] - 1) * Math.pow(2, j);
				}

				if (Math.ceil(tmptaskId) > taskCnt) {
					tmptaskId = taskCnt;
				}

				emittask = (int) Math.ceil(tmptaskId) - 1;

				if (emittask < 0)
					emittask = 0;

				taskSet.add(emittask);

				popflag = 1;

			} else {

				if (curdir + 1 > dimSignBound[curlay]) {

					popflag = 1;

					continue;
				} else {

					if (dimSign[curlay] == 0) {

						tmpCoor[curlay] = orgiCoord[curlay];
						emitStack.add(0);
						stkSize++;

					} else if (dimSign[curlay] == 1) {

						tmpdir = curdir + 1;
						tmpCoor[curlay] = orgiCoord[curlay]
								+ posDirecVec[tmpdir];
						emitStack.add(tmpdir);
						stkSize++;

					} else if (dimSign[curlay] == -1) {

						tmpdir = curdir + 1;
						tmpCoor[curlay] = orgiCoord[curlay]
								+ negDirecVec[tmpdir];
						emitStack.add(tmpdir);
						stkSize++;
					}

					curlay++;
					curdir = -1;
				}
			}
		}

		return;
	}

	public void locateTask2Part(int memidx, int[] dimSign, int[] dimSignBound) {

		int tmpUpBound2 = 0, tmpLowBound2 = 0;

		int[] TaskCoor = new int[TopologyMain.winSize + 5];

		int k = vecst[memidx];
		// while (k != veced[memidx]) {

		for (int j = 0; j < TopologyMain.winh; ++j) {

			dimSign[j] = 0;
			dimSignBound[j] = 0;

			TaskCoor[j] = (cellvec[memidx][k] > 0 ? 2 : 1);

			tmpUpBound2 = (cellvec[memidx][k] + 1);
			if (tmpUpBound2 * cellvec[memidx][k] <= 0) {
				dimSign[j] = 1;
				dimSignBound[j] = 1;
			}

			tmpLowBound2 = (cellvec[memidx][k] - 1);
			if (tmpLowBound2 * cellvec[memidx][k] <= 0) {
				dimSign[j] = -1;
				dimSignBound[j] = 1;
			}

			k = (k + 1) % queueLen;

		}

		// broadcastEmitNoRecurSubDim2Part(TaskCoor, dimSign );

		return;
	}

	@Override
	public void cleanup() {

	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		// TODO Auto-generated method stub

		taskId = context.getThisTaskId();
		localTaskIdx = context.getThisTaskIndex();

		tasks = context.getComponentTasks("llPre");

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

		declarer.declareStream("dataTup", new Fields("id", "strevec", "ts",
				"celltype"));

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

				int[] dimSign = new int[TopologyMain.winSize + 5];
				int[] dimSignBound = new int[TopologyMain.winSize + 5];
				String tmpvec = new String();

				for (i = 0; i < streidCnt; ++i) {

					emitStack.clear();
					taskSet.clear();

					locateTask2Part(i, dimSign, dimSignBound);

					broadcastEmitNoRecurSubDim2Part(cellvec[i], dimSign,
							dimSignBound);

					tmpvec = prepStreVec(i);

					collector.emitDirect(tasks.get(taskSet.get(0)),
							"interStre", new Values(streid[tmppivot], tmpvec,
									curtstamp, 1));

					// collector.emit(tasks.get(taskSet.) ,"interStre", new
					// Values(streid[tmppivot],
					// prepStreVec(i), prepCellVec(i, TopologyMain.winh),
					// curtstamp)); // modification

					for (int j = 1; j < taskSet.size(); ++j) {

						collector.emitDirect(tasks.get(taskSet.get(j)),
								"interStre", new Values(streid[tmppivot],
										tmpvec, curtstamp, 0));
						// collector.emit("interStre", new
						// Values(streid[tmppivot],
						// prepStreVec(i), prepCellVec(i, TopologyMain.winh),
						// curtstamp)); // modification
					}

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
