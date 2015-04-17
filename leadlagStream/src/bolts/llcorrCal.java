package bolts;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import main.TopologyMain;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class llcorrCal extends BaseBasicBolt {

	// ............input time order..............//

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	double curtstamp = TopologyMain.winSize - 1;
	String streType = new String();
	double ts = 0.0;

	String commandStr = new String();
	long preTaskId = 0;

	HashSet<Long> preTaskIdx = new HashSet<Long>();
	// .........memory management for sliding windows.................//

	int[] cellType = new int[TopologyMain.gridIdxN + 1];
	HashMap<Integer, Integer> streIdx = new HashMap<Integer, Integer>();
	HashMap<String, Integer> cellIdx = new HashMap<String, Integer>();
	
	HashMap<String, Queue<String> > cellLagStre = new HashMap<String, Queue<String> >();
	Queue<Integer> queue = new LinkedList<Integer>();
	
	List<List<Double>> streamvec = new ArrayList<List<Double>>(
			TopologyMain.gridIdxN + 1);
	
	List<List<Integer>> cellStre = new ArrayList<List<Integer>>(
			TopologyMain.gridIdxN + 1);

	List<List<Integer>> cellvec = new ArrayList<List<Integer>>(
			TopologyMain.gridIdxN + 1);

	// ...........computation parameter....................//
	int locTaskId, locTaskIdx;
	double disThre = 2 - 2 * TopologyMain.thre;
	double cellEps = Math.sqrt(disThre);

	int recStreCnt = 0;

	// ..........................


	public int vecAnaylMap( int streid, String streamstr) {

		int len = streamstr.length(), cnt = 0, pre = 0;
		double tmpnorm = 0.0;
		String cellstr = new String();
		int cellid=0, tmpcell=0;
        
		ArrayList<Integer> tmpCellVec=new ArrayList<Integer>();
		
		for (int i = 0; i < len; ++i) {
			if (streamstr.charAt(i) == ',') {

				
				tmpnorm = Double.valueOf(streamstr.substring(pre, i));
				streamvec.get(streid).add(tmpnorm);

				if (tmpnorm >= 0) {

					
					// it is floor here
					tmpcell= (int) Math.floor((double) tmpnorm / cellEps);
					tmpCellVec.add(tmpcell);
					
					cellstr = cellstr +Integer.toString(tmpcell)+ ",";
					

				} else {
					
					tmpcell= (int) (-1)* (int) Math.ceil((double) -1 * tmpnorm / cellEps);
					tmpCellVec.add(tmpcell);
					
					cellstr = cellstr +Integer.toString(tmpcell)+ ",";
				}

				pre = i + 1;
			}
		}
		
		
		if(cellIdx.containsKey(cellstr)==true)
		{
			cellid= cellIdx.get(cellstr);
			cellStre.get(cellid).add(streid);
			
		}
		else
		{
			cellid= cellIdx.size();
			cellIdx.put(cellstr, cellid);
			cellvec.add(tmpCellVec);
		}
		
		return cellid;
	}

	public int cellTypeCal(String cellstr) {

		int taskid = 0, tmp = 0;
		
		List<String> list = new ArrayList<String>(Arrays.asList(cellstr.split(",")));
	    int size=list.size();
	    tmp= Integer.parseInt( list.get(0) );
	    
	    taskid = (tmp > 0 ? 1 : 0);
	    
		for (int j = 1; j < size; ++j) {

			tmp= Integer.parseInt( list.get(j) );
			tmp = (tmp > 0 ? 2 : 1);
			taskid = (int) (taskid + (tmp - 1) * Math.pow(2, j));
		}

		return (taskid == locTaskIdx ? 1 : 0);
	}



	public void IndexingStre(int streid, String streamStr, String cellStr, double stamp) {

		int streNo = 0, cellid=0;

		String swid = streid + "," + Double.toString(stamp);// sliding window id

		if (streIdx.containsKey(swid) == true) {

			return;

		} else {
			streNo = streIdx.size();
			streIdx.put(streid, streNo);
		}

		cellid=vecAnaylMap( streNo, streamStr);

		cellType[cellid] = cellTypeCal(cellStr);

		return;
	}

	int correCalDisReal(double thre, int streid1, int streid2) {

		int memidx1 = streIdx.get(streid1), memidx2 = streIdx.get(streid2), k = 0;

		double tmpres = 0.0;

//		for (k = 0; k < TopologyMain.winSize; ++k) {
//			tmpres = tmpres + (streamVec[memidx1][k] - streamVec[memidx2][k])
//					* (streamVec[memidx1][k] - streamVec[memidx2][k]);
//		}

		return tmpres <= thre ? 1 : 0;

	}


	int cellCorrCal(BasicOutputCollector collector) {

		// resPair.clear();
		int rescnt = 0;
		int cellcnt = cellIdx.size();

		for (int i = 0; i < cellcnt; ++i) {

			// rescnt += cellWithinCal(i, collector);
		}
		return rescnt;
	}

	public void localIdxRenew() {

		cellIdx.clear();
		streIdx.clear();

		cellStre.clear();

		return;
	}

	@Override
	public void cleanup() {

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

		declarer.declare(new Fields("ts", "pair"));
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {

		locTaskId = context.getThisTaskId();
		locTaskIdx = context.getThisTaskIndex();

		return;
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub

		// declarer.declareStream("interStre", new Fields("id", "strevec",
		// "cellvec", "ts"));
		// declarer.declareStream("calCommand", new Fields("command",
		// "taskid"));

		streType = input.getSourceStreamId();

		if (streType.compareTo("dataTup") == 0) {

			recStreCnt++;

			ts = input.getDoubleByField("ts");
			String strestr = input.getStringByField("strevec");
			String cellstr = input.getStringByField("cellvec");
			int streid = input.getIntegerByField("id");

			IndexingStre(streid, strestr,cellstr, ts);

		} else if (streType.compareTo("calCommand") == 0) {

			commandStr = input.getStringByField("command");
			preTaskId = input.getLongByField("taskid");

			preTaskIdx.add(preTaskId);
			if (preTaskIdx.size() < TopologyMain.preBoltNum) {
				return;
			}

			recStreCnt = cellCorrCal(collector);

			localIdxRenew();

			curtstamp = ts + 1;
			preTaskIdx.clear();
		}

		return;
	}
}
