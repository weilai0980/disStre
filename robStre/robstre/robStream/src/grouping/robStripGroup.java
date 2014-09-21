package grouping;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import main.TopologyMain;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.TopologyContext;
import backtype.storm.task.WorkerTopologyContext;
import backtype.storm.tuple.Fields;

public class robStripGroup implements CustomStreamGrouping {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	// private Map _map;
	// private TopologyContext _ctx;
	// private Fields _fields;
	private List<Integer> _tasks;

	public double curtstamp = TopologyMain.winSize - 1; // need modification
	public int[] TaskCoor = new int[TopologyMain.winSize + 5];
	public double ts = 0.0;

	public ArrayList<Integer> tasklist = new ArrayList<Integer>();

	public double disThre = 2 - 2 * TopologyMain.thre;
	public double cellEps = Math.sqrt(disThre);

	public int taskCnt = 0;
	
	public int repType;  
	public int taskcoor;
	public int gap;
	
	public int taskBound = (int) Math.floor(1.0/cellEps)*TopologyMain.cellTask+1; 
	

	@Override
	public void prepare(WorkerTopologyContext context, GlobalStreamId stream,
			List<Integer> targetTasks) {
		// TODO Auto-generated method stub

		_tasks = targetTasks;
		taskCnt = _tasks.size();

	}

	public Set <Integer> taskSet = new HashSet<Integer>();

	public void broadcastTask(int taskcoor, int gap, int type) {
		
		int tmpcoor=0;
		
		if(type==0)
		{
			for(int i=0;i<TopologyMain.cellTask;i++)
			{
				tmpcoor=taskcoor+i;
				
				if(taskcoor+i< 2*taskBound)
				{
				taskSet.add(_tasks.get(tmpcoor));
				}
			}
		}
		else if(type == -1)
		{
		
			for(int i=0;i<gap;i++)
			{
				tmpcoor=taskcoor+i;
				
				if(taskcoor+i< 2*taskBound)
				{
				taskSet.add(_tasks.get(tmpcoor));
				}
			}
			
		}
		else if(type==-10)
		{
			
			for(int i=gap;i<TopologyMain.cellTask;i++)
			{
				tmpcoor=taskcoor+i;
				
				if(taskcoor+i< 2*taskBound)
				{
				taskSet.add(_tasks.get(tmpcoor));
				}
			}
			
		}
		else if(type==1)
		{
			for(int i=TopologyMain.cellTask-gap;i<TopologyMain.cellTask;i++)
			{
				tmpcoor=taskcoor+i;
				
				if(taskcoor+i< 2*taskBound)
				{
				taskSet.add(_tasks.get(tmpcoor));
				}
			}
		}
		else if(type==10)
		{
			for(int i=0;i<TopologyMain.cellTask-gap;i++)
			{
				tmpcoor=taskcoor+i;
				
				if(taskcoor+i< 2*taskBound)
				{
				taskSet.add(_tasks.get(tmpcoor));
				}
			}
		}
		return;
	}

	@Override
	public List<Integer> chooseTasks(int taskId, List<Object> values) {
		// TODO Auto-generated method stub
	
//		declarer.declare(new Fields("repType", "strevec", "ts", "taskCoor",
//				"streId", "gap"));
	
		repType=Integer.valueOf(values.get(0).toString());
		taskcoor=Integer.valueOf(values.get(3).toString());
		gap=Integer.valueOf(values.get(5).toString());
		ts = Double.valueOf(values.get(2).toString());

		tasklist.clear();

		if (ts > curtstamp) {
			
			taskSet.clear();
			broadcastTask(taskcoor, gap, repType);
			
			for (Integer tmp : taskSet) {
				tasklist.add(tmp);
			}
			
			curtstamp = ts;
			
			return tasklist;

		} else if (ts < curtstamp) {
			System.out
					.printf("!!!!!!!!!!!!! inter robStripGroup time sequence disorder at %f %f\n",
							ts, curtstamp);

			return tasklist;

		} else {

			taskSet.clear();
			
			broadcastTask(taskcoor, gap, repType);
		
			for (Integer tmp : taskSet) {
				tasklist.add(tmp);
			}

			return tasklist;
		}
	}
}
