package grouping;

import main.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import main.TopologyMain;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.TopologyContext;
import backtype.storm.task.WorkerTopologyContext;
import backtype.storm.tuple.Fields;

public class AdjustGroup implements CustomStreamGrouping {

	// private Map _map;
	private TopologyContext _ctx;
	private Fields _fields;
	private List<Integer> _tasks;

	public int tupcnt = 0;
	public static int groupid = 0;
	public int localgid = 0;

	public double curtstamp = TopologyMain.winSize - 1; // need modification

	public int[] tmpGridCoor = new int[TopologyMain.winSize + 5];
	public int[] tmpTaskMBRmin = new int[TopologyMain.winSize + 5];
	public int[] tmpTaskMBRmax = new int[TopologyMain.winSize + 5];

	public int[][] taskGridCoorMin = new int[TopologyMain.tasknum + 5][TopologyMain.winSize + 5];
	public int[][] taskGridCoorMax = new int[TopologyMain.tasknum + 5][TopologyMain.winSize + 5];
	public int taskCnt = 0, i = 0, j = 0;

	public double ts = 0.0;
	public String coorstr = new String();

	public ArrayList<Integer> tasklist = new ArrayList<Integer>();
	
	public int gridcnt=0;

	@Override
	public void prepare(WorkerTopologyContext context, GlobalStreamId stream,
			List<Integer> targetTasks) {
		// TODO Auto-generated method stub

		_tasks = targetTasks;

		localgid = groupid;
		groupid++;

//		System.out.printf("$$$$$$$$$$$$$  current groupers: %d\n", groupid);

	}

	void coorAna(String str, int coor[]) {
		int l = str.length();
		int pre = 0, tmpcoor = 0, cnt = 0;

		for (int i = 0; i < l; ++i) {
			if (str.charAt(i) == ',') {
				tmpcoor = Integer.valueOf(str.substring(pre, i));
				coor[cnt++] = tmpcoor;
				pre = i + 1;
			}
		}
		return;
	}

	@Override
	public List<Integer> chooseTasks(int taskId, List<Object> values) {
		// TODO Auto-generated method stub

		// declarer.declareStream("interStre",new Fields("pidx","pivotvec",
		// "adjaffine", "adjidx","coord","ts"));

		coorstr = values.get(4).toString();
		ts = Double.valueOf(values.get(5).toString());
		
		// int idx= (int)(key%_tasks.size());
		coorAna(coorstr, tmpGridCoor);
		tasklist.clear();
		
		gridcnt++;
		
		if (ts > curtstamp) {
			
			
			
			//.........test...........//
			
			System.out
			.printf("Group ID  %d  at time stamp at %f total grid: %d\n",localgid,curtstamp,gridcnt-1);
			
			gridcnt=0;
			
			//.......................//
			
			

			
			for (int i = 0; i < TopologyMain.winSize; ++i) {
				taskGridCoorMin[0][i] = tmpGridCoor[i] - 1;
				taskGridCoorMax[0][i] = tmpGridCoor[i] + 1;
			}
			tasklist.add(_tasks.get(0));

			taskCnt = 1;
			curtstamp = ts;

			return tasklist;
			// return Arrays.asList(_tasks.get(0));

		} else if (ts < curtstamp) {
			System.out
					.printf("!!!!!!!!!!!!! inter AdjustGroup time sequence disorder at %f %f\n",ts,curtstamp);

			return tasklist;
			// return Arrays.asList(_tasks.get(0));

		} else {

			int assigned = 0;
			for (i = 0; i < taskCnt; i++) {
				for (j = 0; j < TopologyMain.winSize; ++j) {
					if ((tmpGridCoor[j] + 1) < taskGridCoorMin[i][j]
							|| (tmpGridCoor[j] - 1) > taskGridCoorMax[i][j]) {
						break;
					} else {
						tmpTaskMBRmin[j] = Math.min(tmpGridCoor[j] - 1,
								taskGridCoorMin[i][j]);
						tmpTaskMBRmax[j] = Math.max(tmpGridCoor[j] + 1,
								taskGridCoorMax[i][j]);

					}
				}
				if (j == TopologyMain.winSize) {
					assigned = 1;

					
//					System.out.printf("!!!!!!  Time at %f: get in\n",curtstamp);
					
					
					
					tasklist.add(_tasks.get(i));

					for (j = 0; j < TopologyMain.winSize; ++j) {
						taskGridCoorMin[i][j] = tmpTaskMBRmin[j];
						taskGridCoorMax[i][j] = tmpTaskMBRmax[j];
					}

				}

			}
			if (assigned == 0) {

				if (taskCnt < TopologyMain.tasknum) {
					for (int i = 0; i < TopologyMain.winSize; ++i) {
						taskGridCoorMin[taskCnt][i] = tmpGridCoor[i] - 1;
						taskGridCoorMax[taskCnt][i] = tmpGridCoor[i] + 1;
					}
					tasklist.add(_tasks.get(taskCnt));
					taskCnt++;

				} else {
					for (int i = 0; i < TopologyMain.winSize; ++i) {
						taskGridCoorMin[TopologyMain.tasknum-1][i] = Math.min(
								tmpGridCoor[i] - 1, taskGridCoorMin[TopologyMain.tasknum-1][i]);
						taskGridCoorMax[TopologyMain.tasknum-1][i] = Math.max(
								tmpGridCoor[i] + 1, taskGridCoorMax[TopologyMain.tasknum-1][i]);
					}
					tasklist.add(_tasks.get(TopologyMain.tasknum-1));
//					taskCnt++;

				}

			}
			return tasklist;

			// long key=Long.valueOf( values.get(0).toString());

			// int idx= (int)(key%_tasks.size());
			// return Arrays.asList(_tasks.get(idx));

		}
		// return tasklist;
	}

}
