package grouping;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import main.TopologyMain;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;
import backtype.storm.tuple.Fields;

public class paraCoverGroup implements CustomStreamGrouping {

	/**
	 * 
	 */

	// ...............choose task. ...........................//
	private static final long serialVersionUID = 1L;
	// private Map _map;
	// private TopologyContext _ctx;
	// private Fields _fields;

	private List<Integer> _tasks;

	public ArrayList<Integer> tasklist = new ArrayList<Integer>();
	public Set<Integer> taskSet = new HashSet<Integer>();

	// ..............time order.............................//
	public double ts = 0.0;

	// ...........computation parameter....................//
	public final double disThre = 2 - 2 * TopologyMain.thre;
	public final double cellEps = Math.sqrt(disThre);
	public final int hSpaceTaskNum = (int) Math.floor(1.0 / cellEps)
			* TopologyMain.cellTask + 1;
	public final int hSpaceCellNum = (int) Math.ceil(1.0 / cellEps);

	// ............tuple data......................//

	public int repType;
	public int taskcoor;
	public int gap;
	int streid = 0;

	@Override
	public void prepare(WorkerTopologyContext context, GlobalStreamId stream,
			List<Integer> targetTasks) {
		// TODO Auto-generated method stub

		_tasks = targetTasks;
	}

	public void broadcastTask(int taskcoor, int gap, int type) {

		int tmpcoor = 0;

		if (type == 0) {
			for (int i = 0; i <= TopologyMain.cellTask; i++) {
				tmpcoor = taskcoor + i;

				if (taskcoor + i < 2 * hSpaceTaskNum) {
					taskSet.add(_tasks.get(tmpcoor));
				}
			}
		} else if (type == -1) {

			if (gap > TopologyMain.cellTask) {
				for (int i = 0; i <= TopologyMain.cellTask; i++) {
					tmpcoor = taskcoor + i;

					if (taskcoor + i < 2 * hSpaceTaskNum) {
						taskSet.add(_tasks.get(tmpcoor));
					}
				}
			} else {

				for (int i = 0; i < gap; i++) {
					tmpcoor = taskcoor + i;

					if (taskcoor + i < 2 * hSpaceTaskNum) {
						taskSet.add(_tasks.get(tmpcoor));
					}
				}
			}

		} else if (type == -10) {

			for (int i = gap; i <= TopologyMain.cellTask; i++) {
				tmpcoor = taskcoor + i;

				if (taskcoor + i < 2 * hSpaceTaskNum) {
					taskSet.add(_tasks.get(tmpcoor));
				}
			}

		} else if (type == 1) {

			if (gap > TopologyMain.cellTask) {
				for (int i = 0; i <= TopologyMain.cellTask; i++) {
					tmpcoor = taskcoor + i;
					if (taskcoor + i < 2 * hSpaceTaskNum) {
						taskSet.add(_tasks.get(tmpcoor));
					}

					// .............test........
					// if(ts==5 && streid==14)
					// {
					// System.out.printf("At time %f, shuffling send stream %d to task %d\n",
					// ts, streid, tmpcoor );
					// }

					// .........................

				}

			} else {
				for (int i = TopologyMain.cellTask - gap + 1; i <= TopologyMain.cellTask; i++) {
					tmpcoor = taskcoor + i;

					if (taskcoor + i < 2 * hSpaceTaskNum) {
						taskSet.add(_tasks.get(tmpcoor));
					}
				}

			}

		} else if (type == 10) {

			for (int i = 0; i < TopologyMain.cellTask - gap + 1; i++) {
				tmpcoor = taskcoor + i;

				if (taskcoor + i < 2 * hSpaceTaskNum) {
					taskSet.add(_tasks.get(tmpcoor));
				}
			}
		}
		return;
	}

	@Override
	public List<Integer> chooseTasks(int taskId, List<Object> values) {
		// TODO Auto-generated method stub

		// declarer.declareStream("streamData", new Fields("dataType", "data",
		// "ts", "task", "streId"));

		taskcoor = Integer.valueOf(values.get(3).toString());

		tasklist.add(taskcoor);

		return tasklist;
	}
}
