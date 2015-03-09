package grouping;

import java.util.ArrayList;
import java.util.List;

import main.TopologyMain;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.TopologyContext;
import backtype.storm.task.WorkerTopologyContext;
import backtype.storm.tuple.Fields;

public class AdjustHashGroup implements CustomStreamGrouping {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	// private Map _map;
//	private TopologyContext _ctx;
//	private Fields _fields;
	private List<Integer> _tasks;

	public static int groupid = 0;
	public int localgid = 0;

	public double curtstamp = TopologyMain.winSize - 1; // need modification

	public int[] tmpGridCoor = new int[TopologyMain.winSize + 5];
	public int[] TaskCoor = new int[TopologyMain.winSize + 5];
	
	public int[] tmpBroadCoor = new int[TopologyMain.winSize + 5];

	public int[] dimSign = new int[TopologyMain.winSize + 5];

	public double ts = 0.0;
	public String coorstr = new String();

	public ArrayList<Integer> tasklist = new ArrayList<Integer>();

	// public int diviNum = (int) Math.floor(Math.getExponent(Math
	// .log(TopologyMain.approBoltNum) / TopologyMain.winSize)); // division on
	// each dimension

	public double diviNum = Math.exp(Math.log(TopologyMain.calBoltNum)
			/ TopologyMain.winSize); // division on each dimension

	public double taskRange = 2.0 / diviNum;
	public double disThre = 2 - 2 * TopologyMain.thre;

	public int gridRange = (int) Math.ceil(1.0 / Math.sqrt(disThre));
	// public int taskGridMax=2*gridRange-1;

	
	public double taskGridCap = taskRange / Math.sqrt(disThre); 
	
//	public int taskGridCap = (int) Math.ceil(taskRange / Math.sqrt(disThre)); // each
																				// task
																				// space
																				// includes
																				// how
																				// many
																				// grid

	public int gridcnt = 0;
	public int taskCnt = 0;
	public int broadcnt=0;
	
	public int emittask=0;
	
	//................test..................//
//	String adjList=new String();
//	int pivotId=0;
//	String pivotVec=new String();
////	
//	public int ta=3,tb=8;
//	public double tt=21;
	//......................................//

	@Override
	public void prepare(WorkerTopologyContext context, GlobalStreamId stream,
			List<Integer> targetTasks) {
		// TODO Auto-generated method stub

		_tasks = targetTasks;
		taskCnt = _tasks.size();

		localgid = groupid;
		groupid++;

		// Math.e
		// System.out.printf("-------- %f  %f  %d  \n",
		// diviNum,taskRange,gridRange);
	}

	public void coorAna(String str, int coor[]) {
		int l = str.length();
		int pre = 0, tmpcoor = 0, cnt = 0;

		for (int i = 0; i < l; ++i) {
			if (str.charAt(i) == ',') {
				tmpcoor = Integer.valueOf(str.substring(pre, i));
				// coor[cnt++] = tmpcoor;

				coor[cnt++] = tmpcoor + gridRange + 1; // coordinate
														// transposition to zero
														// point
				pre = i + 1;
			}
		}
		return;
	}

//	public int adjIdx(String orgstr) {
//
//		int len = orgstr.length();
//		int pre = 0;
//		int tmpid;
//		for (int i = 0; i < len; ++i) {
//			if (orgstr.charAt(i) == ',') {
//				tmpid = Integer.valueOf(orgstr.substring(pre, i));
//				
//				if(tmpid==ta || tmpid==tb)
//				{
//					return tmpid;
//				}
//				
//				pre = i + 1;
//			}
//		}
//		return -1;
//	}
	
	public void locateTask() {
		int tmpTaskCoor = 0;
//		int tmpUpBound1 = 0, tmpLowBound1 = 0, 
		int tmpUpBound2 = 0, tmpLowBound2 = 0;

		// int taskid = 0;

		// System.out.printf("++++++++++++++++++++++++  ");
		
		int intDivinum=(int)Math.ceil(diviNum);
		

		for (int j = 0; j < TopologyMain.winSize; ++j) {

			dimSign[j] = 0;

			tmpTaskCoor = (int) Math
					.ceil((double) tmpGridCoor[j] / taskGridCap);

			TaskCoor[j] = tmpTaskCoor;

//			tmpUpBound1 = (int) Math.ceil((double) (tmpGridCoor[j] + 1)
//					/ taskGridCap);
//			tmpLowBound1 = (int) Math.ceil((double) (tmpGridCoor[j] - 1)
//					/ taskGridCap);

			tmpUpBound2 = (int) Math.ceil((double) (tmpGridCoor[j] + 2) // modification
					/ taskGridCap);
//			tmpLowBound2 = (int) Math.ceil((double) (tmpGridCoor[j] - 2)
//					/ taskGridCap);
			
			tmpLowBound2 = (int) Math.ceil((double) (tmpGridCoor[j] - 2) // modification
					/ taskGridCap);

			// if(tmpTaskCoor==diviNum || diviNum==1)
			// {
			// dimSign[j] = 0;
			// }

			if (tmpUpBound2 > intDivinum) {
				dimSign[j] = 0;
			} else if (tmpLowBound2 < 1) {
				dimSign[j] = 0;
			} else if (tmpUpBound2 > tmpTaskCoor ) {
				dimSign[j] = 1;
			} else if (tmpLowBound2 < tmpTaskCoor ) {
				dimSign[j] = -1;
			}

			// System.out.printf("%d  %d   %d   %d;",
			// tmpGridCoor[j],taskGridCap, TaskCoor[j],dimSign[j] );

			// if (j == 0) {
			// taskid = (tmpTaskCoor) * (int) Math.pow(diviNum, j);
			// } else {
			// taskid = (tmpTaskCoor - 1) * (int) Math.pow(diviNum, j);
			// }

		}
		// System.out.printf("\n");
		// tasklist.add(_tasks.get(taskid));

		return;
	}

	public void broadcastTask(int curdim, double tmptaskId) {

		if (curdim == TopologyMain.winSize) {
			
			broadcnt++;

			if (Math.ceil(tmptaskId) > taskCnt) {
				tmptaskId = taskCnt;
			}

			//...........test.....................//
//			System.out
//			.printf("grid coordinate at timestamp %f\n",curtstamp);
//			for(int i=0;i<TopologyMain.winSize;i++)
//			{
//				System.out
//				.printf("%d   ",tmpBroadCoor[i]);
//			}
//			
//			System.out.printf(" : task id   %d\n",(int) Math.ceil(tmptaskId));
			
//			if(curtstamp==tt && (pivotId==ta || pivotId==tb))
//			{
//				System.out.printf(" hash process:   pivot stream %d  taskid:  %f \n",pivotId, tmptaskId);
//			}
//			
			//........................................//
			
			emittask=(int) Math.ceil(tmptaskId) - 1;
			
//			if(emittask<0) emittask=0;

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
			
			tmpBroadCoor[curdim]=TaskCoor[curdim];
			broadcastTask(curdim + 1,
					tmptaskId + tmpcoor * Math.pow(diviNum, curdim));

		} else if (dimSign[curdim] == 1) {

			
			tmpBroadCoor[curdim]=TaskCoor[curdim]+1;
			
			broadcastTask(curdim + 1,
					tmptaskId + (tmpcoor + 1) * Math.pow(diviNum, curdim));

			tmpBroadCoor[curdim]=TaskCoor[curdim];
			broadcastTask(curdim + 1,
					tmptaskId + tmpcoor * Math.pow(diviNum, curdim));

		} else if (dimSign[curdim] == -1) {

			
			tmpBroadCoor[curdim]=TaskCoor[curdim]-1;
			broadcastTask(curdim + 1,
					tmptaskId + (tmpcoor - 1) * Math.pow(diviNum, curdim));

			tmpBroadCoor[curdim]=TaskCoor[curdim];
			broadcastTask(curdim + 1,
					tmptaskId + tmpcoor * Math.pow(diviNum, curdim));
		}

	}

	@Override
	public List<Integer> chooseTasks(int taskId, List<Object> values) {
		// TODO Auto-generated method stub

//		declarer.declareStream("interStre", new Fields("pidx", "pivotvec",
//				"adjaffine", "adjidx", "coord", "ts", "bolt"));

		
		coorstr = values.get(4).toString();
		ts = Double.valueOf(values.get(5).toString());

		coorAna(coorstr, tmpGridCoor);
		tasklist.clear();

		gridcnt++;
		
		
		

		if (ts > curtstamp) {


			locateTask();
			
			//...............test....................//
//			int isQual=0,tmp=0;
//			adjList= values.get(3).toString();
//			pivotId=Integer.valueOf(values.get(0).toString());
//			pivotVec=values.get(1).toString();
//			
//			if(ts==tt && (pivotId==ta || pivotId==tb))
//			{
//				isQual=1;
//				tmp=pivotId;
//			}
////			else if (ts==tt)
////			{
////				if( (tmp=adjIdx(adjList))!=-1)
////				{
////					isQual=1;
////				}
////			}
//			if(isQual==1)
//			{
//				System.out.printf("+++++++ test stream %d  in  pivot stream %d : ",tmp, pivotId);	
//				for(int k=0;k< TopologyMain.winSize; ++k)
//				{
//					System.out.printf(" %d  ", TaskCoor[k]);
//				}
//				System.out.printf("    ");
//				for(int k=0;k< TopologyMain.winSize; ++k)
//				{
//					System.out.printf(" %d  ", tmpGridCoor[k]);
//				}
//				System.out.printf("    ");
//				for(int k=0;k< TopologyMain.winSize; ++k)
//				{
//					System.out.printf(" %d  ", dimSign[k]);
//				}
//				System.out.printf("\n");
//				
//			}
//			
//			
//			if(pivotId==0 && ts==tt)
//			{
//				System.out.printf(" #########  affines streams of pivot stream:   %s \n ",adjList);	
//			}
			
			//........................................//
			
			
			
			broadcastTask(0, 0.0);
			


			gridcnt = 0;
			curtstamp = ts;


			return tasklist;

		} else if (ts < curtstamp) {
			System.out
					.printf("!!!!!!!!!!!!! inter AdjustGroup time sequence disorder at %f %f\n",
							ts, curtstamp);

			return tasklist;

		} else {

			locateTask();
			
			//...............test....................//
//			int isQual=0,tmp=0;
//			adjList= values.get(3).toString();
//			pivotId=Integer.valueOf(values.get(0).toString());
//			pivotVec=values.get(1).toString();
////			
//			if(curtstamp==tt && (pivotId==ta || pivotId==tb))
//			{
//				isQual=1;
//				tmp=pivotId;
//			}
////			else if (curtstamp==tt)
////			{
////				if( (tmp=adjIdx(adjList))!=-1)
////				{
////					isQual=1;
////				}
////			}
//			if(isQual==1)
//			{
//				System.out.printf("background coeffi:  %f  \n",taskGridCap);
//				
//				
//				System.out.printf("+++++++ test stream %d  in  pivot stream %d : ",tmp, pivotId);	
//				for(int k=0;k< TopologyMain.winSize; ++k)
//				{
//					System.out.printf(" %d  ", TaskCoor[k]);
//				}
//				System.out.printf("    ");
//				for(int k=0;k< TopologyMain.winSize; ++k)
//				{
//					System.out.printf(" %d  ", tmpGridCoor[k]);
//				}
//				System.out.printf("    ");
//				for(int k=0;k< TopologyMain.winSize; ++k)
//				{
//					System.out.printf(" %d  ", dimSign[k]);
//				}
//				
//				System.out.printf("\n");
//			}
//			
//			
//			
//			
//			if(pivotId==0 && curtstamp==tt)
//			{
//				System.out.printf(" +  #########  affines streams of pivot stream:   %s \n ",adjList);	
//			}
			
			//........................................//
		
			
			broadcastTask(0, 0.0);
			

			

			return tasklist;

		}

	}
}
