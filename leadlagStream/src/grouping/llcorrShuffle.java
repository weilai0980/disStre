package grouping;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import main.TopologyMain;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;

public class llcorrShuffle implements CustomStreamGrouping {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	// private Map _map;
	// private TopologyContext _ctx;
	// private Fields _fields;
	private List<Integer> _tasks;

	public static int groupid = 0;
	public int localgid = 0;

	public double curtstamp = TopologyMain.winSize - 1; // need modification

	public int[] tmpGridCoor = new int[TopologyMain.winSize + 5];
	public int[] TaskCoor = new int[TopologyMain.winSize + 5];

	public int[] tmpBroadCoor = new int[TopologyMain.winSize + 5];

	public int[] dimSign = new int[TopologyMain.winSize + 5];

	public int[] dimSignBound = new int[TopologyMain.winSize + 5];

	public double ts = 0.0;
	public String coorstr = new String();

	public ArrayList<Integer> tasklist = new ArrayList<Integer>();

	public double diviNum = Math.exp(Math.log(TopologyMain.calBoltNum)
			/ TopologyMain.winSize); // division on each dimension

	public double subDivNum = 2;

	public double taskRange = 2.0 / subDivNum;

	public double disThre = 2 - 2 * TopologyMain.thre;

	public int gridRange = (int) Math.ceil(1.0 / Math.sqrt(disThre));

	public double taskGridCap = taskRange / Math.sqrt(disThre);

	public int gridcnt = 0;
	public int taskCnt = 0;
	public int broadcnt = 0;

	public int emittask = 0;

	@Override
	public void prepare(WorkerTopologyContext context, GlobalStreamId stream,
			List<Integer> targetTasks) {
		// TODO Auto-generated method stub

		_tasks = targetTasks;
		taskCnt = _tasks.size();

		localgid = groupid;
		groupid++;

	}

	public void coorAna(String str, int coor[]) {
		int l = str.length();
		int pre = 0, tmpcoor = 0, cnt = 0;

		for (int i = 0; i < l; ++i) {
			if (str.charAt(i) == ',') {
				tmpcoor = Integer.valueOf(str.substring(pre, i));
				// coor[cnt++] = tmpcoor;

				coor[cnt++] = tmpcoor + gridRange + 1; // coordinate
														// transposition to
														// positive range

				pre = i + 1;
			}
		}
		return;
	}

	public void locateTask() {
		int tmpTaskCoor = 0;

		int tmpUpBound2 = 0, tmpLowBound2 = 0;

		// .......still needed................//

		// tmpUpBound1 = (int) Math.ceil((double) (tmpGridCoor[j] + 1)
		// / taskGridCap);
		// tmpLowBound1 = (int) Math.ceil((double) (tmpGridCoor[j] - 1)
		// / taskGridCap);

//		int intDivinum = (int) Math.ceil(diviNum);

		for (int j = 0; j < TopologyMain.winSize; ++j) {

			dimSign[j] = 0;
			dimSignBound[j] = 0;

			tmpTaskCoor = (int) Math
					.ceil((double) tmpGridCoor[j] / taskGridCap);

			TaskCoor[j] = tmpTaskCoor;

			tmpUpBound2 = (int) Math.ceil((double) (tmpGridCoor[j] + 1) // modification
					/ taskGridCap);

			tmpLowBound2 = (int) Math.ceil((double) (tmpGridCoor[j] - 1) // modification
					/ taskGridCap);

			if (tmpUpBound2 > tmpTaskCoor && tmpUpBound2 <= subDivNum) {

				dimSign[j] = 1;
				dimSignBound[j] = 1;
			}
			if (tmpLowBound2 < tmpTaskCoor && tmpLowBound2 >= 1) {

				if (dimSign[j] == 1) {
					dimSign[j] = 2;
					dimSignBound[j] = 2;
				} else {
					dimSign[j] = -1;
					dimSignBound[j] = 1;
				}
			}

		}

		return;
	}

	public void locateTask2Part() {

		int tmpUpBound2 = 0, tmpLowBound2 = 0;

		for (int j = 0; j < TopologyMain.winSize; ++j) {

			dimSign[j] = 0;
			dimSignBound[j] = 0;

			TaskCoor[j] = (tmpGridCoor[j] > 0 ? 2 : 1);

			tmpUpBound2 = (tmpGridCoor[j] + 2);
			if (tmpUpBound2 * tmpGridCoor[j] <= 0) {
				dimSign[j] = 1;
				dimSignBound[j] = 1;
			}

			tmpLowBound2 = (tmpGridCoor[j] - 2);
			if (tmpLowBound2 * tmpGridCoor[j] <= 0) {
				dimSign[j] = -1;
				dimSignBound[j] = 1;
			}

		}

		return;
	}
	public ArrayList<Integer> emitStack = new ArrayList<Integer>();

	public Set<Integer> taskSet = new HashSet<Integer>();

	public int[] direcVec = { -1, 0, 1 };

	public void broadcastEmitNoRecur(int orgiCoord[]) {

		int curlay = 0;
		int[] tmpCoor = new int[TopologyMain.winSize + 5];

		int stkSize = 0, curdir = -1, tmpdir = 0;
		double tmptaskId = 0.0;

		// .....ini....................//
		if (dimSignBound[curlay] == 0) {
			tmpCoor[curlay] = orgiCoord[curlay];

			emitStack.add(0);
			stkSize++;
		} else if (dimSignBound[curlay] == 1) {

			tmpdir = curdir + 1;
			tmpCoor[curlay] = orgiCoord[curlay] + tmpdir * dimSign[curlay];

			emitStack.add(0);
			stkSize++;
		} else if (dimSignBound[curlay] == 2) {

			tmpdir = curdir + 1;
			tmpCoor[curlay] = orgiCoord[curlay] + direcVec[tmpdir];

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

			if (curlay >= TopologyMain.winSize) {

				tmptaskId = tmpCoor[0];
				for (int j = 1; j < TopologyMain.winSize; ++j) {
					tmptaskId = tmptaskId + (tmpCoor[j] - 1)
							* Math.pow(diviNum, j);
				}

				if (Math.ceil(tmptaskId) > taskCnt) {
					tmptaskId = taskCnt;
				}

				emittask = (int) Math.ceil(tmptaskId) - 1;

				if (emittask < 0)
					emittask = 0;

				taskSet.add(_tasks.get(emittask));

				popflag = 1;

			} else {

				if (curdir + 1 > dimSignBound[curlay]) {

					popflag = 1;

					continue;
				} else {

					if (dimSignBound[curlay] == 0) {
						tmpCoor[curlay] = orgiCoord[curlay];

						emitStack.add(0);
						stkSize++;

					} else if (dimSignBound[curlay] == 1) {

						tmpdir = curdir + 1;
						tmpCoor[curlay] = orgiCoord[curlay] + tmpdir
								* dimSign[curlay];

						emitStack.add(tmpdir);
						stkSize++;

					} else if (dimSignBound[curlay] == 2) {

						tmpdir = curdir + 1;
						tmpCoor[curlay] = orgiCoord[curlay] + direcVec[tmpdir];

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

	public void broadcastEmitNoRecurSubDim(int orgiCoord[]) {

		int curlay = 0;
		int[] tmpCoor = new int[TopologyMain.winSize + 5];

		int stkSize = 0, curdir = -1, tmpdir = 0;
		double tmptaskId = 0.0;

		// .....ini....................//
		if (dimSignBound[curlay] == 0) {
			tmpCoor[curlay] = orgiCoord[curlay];

			emitStack.add(0);
			stkSize++;
		} else if (dimSignBound[curlay] == 1) {

			tmpdir = curdir + 1;
			tmpCoor[curlay] = orgiCoord[curlay] + tmpdir * dimSign[curlay];
			// tmpCoor[curlay] = orgiCoord[curlay] + dimSign[curlay];

			emitStack.add(0);
			stkSize++;
		} else if (dimSignBound[curlay] == 2) {

			tmpdir = curdir + 1;
			tmpCoor[curlay] = orgiCoord[curlay] + direcVec[tmpdir];

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
					tmptaskId = tmptaskId + (tmpCoor[j] - 1)
							* Math.pow(subDivNum, j);
				}

				if (Math.ceil(tmptaskId) > taskCnt) {
					tmptaskId = taskCnt;
				}

				emittask = (int) Math.ceil(tmptaskId) - 1;

				if (emittask < 0)
					emittask = 0;

				taskSet.add(_tasks.get(emittask));

				popflag = 1;

			} else {

				if (curdir + 1 > dimSignBound[curlay]) {

					popflag = 1;

					continue;
				} else {

					if (dimSignBound[curlay] == 0) {
						tmpCoor[curlay] = orgiCoord[curlay];

						emitStack.add(0);
						stkSize++;

					} else if (dimSignBound[curlay] == 1) {

						tmpdir = curdir + 1;
						tmpCoor[curlay] = orgiCoord[curlay] + tmpdir
								* dimSign[curlay];

						emitStack.add(tmpdir);
						stkSize++;

					} else if (dimSignBound[curlay] == 2) {

						tmpdir = curdir + 1;
						tmpCoor[curlay] = orgiCoord[curlay] + direcVec[tmpdir];

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

	public void broadcastTask(int curdim, double tmptaskId) {

		if (curdim == TopologyMain.winSize) {

			broadcnt++;

			if (Math.ceil(tmptaskId) > taskCnt) {
				tmptaskId = taskCnt;
			}

			emittask = (int) Math.ceil(tmptaskId) - 1;

			tasklist.add(_tasks.get(emittask));

			return;
		}

		int tmpcoor = 0;
		if (curdim == 0) {
			tmpcoor = (TaskCoor[curdim]);
		} else {
			tmpcoor = (TaskCoor[curdim] - 1);
		}

		if (dimSign[curdim] == 0) {

			tmpBroadCoor[curdim] = TaskCoor[curdim];
			broadcastTask(curdim + 1,
					tmptaskId + tmpcoor * Math.pow(diviNum, curdim));

		} else if (dimSign[curdim] == 1) {

			tmpBroadCoor[curdim] = TaskCoor[curdim] + 1;

			broadcastTask(curdim + 1,
					tmptaskId + (tmpcoor + 1) * Math.pow(diviNum, curdim));

			tmpBroadCoor[curdim] = TaskCoor[curdim];
			broadcastTask(curdim + 1,
					tmptaskId + tmpcoor * Math.pow(diviNum, curdim));

		} else if (dimSign[curdim] == -1) {

			tmpBroadCoor[curdim] = TaskCoor[curdim] - 1;
			broadcastTask(curdim + 1,
					tmptaskId + (tmpcoor - 1) * Math.pow(diviNum, curdim));

			tmpBroadCoor[curdim] = TaskCoor[curdim];
			broadcastTask(curdim + 1,
					tmptaskId + tmpcoor * Math.pow(diviNum, curdim));
		} else if (dimSign[curdim] == 2) {

			tmpBroadCoor[curdim] = TaskCoor[curdim] - 1;
			broadcastTask(curdim + 1,
					tmptaskId + (tmpcoor - 1) * Math.pow(diviNum, curdim));

			tmpBroadCoor[curdim] = TaskCoor[curdim];
			broadcastTask(curdim + 1,
					tmptaskId + tmpcoor * Math.pow(diviNum, curdim));

			tmpBroadCoor[curdim] = TaskCoor[curdim] + 1;
			broadcastTask(curdim + 1,
					tmptaskId + (tmpcoor + 1) * Math.pow(diviNum, curdim));
		}

	}

	@Override
	public List<Integer> chooseTasks(int taskId, List<Object> values) {
		// TODO Auto-generated method stub

		// declarer.declareStream("interStre", new Fields("id", "strevec",
		// "cellvec", "ts"));

		coorstr = values.get(2).toString();
		ts = Double.valueOf(values.get(3).toString());

		coorAna(coorstr, tmpGridCoor);
		tasklist.clear();

		gridcnt++;

		if (ts > curtstamp) {

			locateTask2Part();

			emitStack.clear();
			taskSet.clear();

			broadcastEmitNoRecurSubDim(TaskCoor);
			// broadcastEmitNoRecur(TaskCoor);

			for (Integer tmp : taskSet) {
				tasklist.add(tmp);
			}

			curtstamp = ts;

			return tasklist;

		} else if (ts < curtstamp) {
			System.out
					.printf("!!!!!!!!!!!!! inter AdjustGroup time sequence disorder at %f %f\n",
							ts, curtstamp);

			return tasklist;

		} else {

			locateTask2Part();

			emitStack.clear();
			taskSet.clear();

			broadcastEmitNoRecurSubDim(TaskCoor);
			// broadcastEmitNoRecur(TaskCoor);

			for (Integer tmp : taskSet) {
				tasklist.add(tmp);
			}

			return tasklist;

		}

	}
}