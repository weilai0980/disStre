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
import backtype.storm.tuple.Values;

public class llcorrCal extends BaseBasicBolt {

	// ............input time order..............//

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	double curtstamp = TopologyMain.winSize - 1;
	String streType = new String();
	double ts = 0.0;
	private int celltype = 0;

	String commandStr = new String();
	long preTaskId = 0;

	HashSet<Long> preTaskIdx = new HashSet<Long>();
	// .........memory management for sliding windows.................//

	HashMap<String, ArrayList<Integer>> cellVec = new HashMap<String, ArrayList<Integer>>();
	HashMap<String, ArrayList<Double>> swVec = new HashMap<String, ArrayList<Double>>();
	HashMap<String, Queue<String>> cellSw = new HashMap<String, Queue<String>>();
	HashMap<String, Integer> cellType = new HashMap<String, Integer>();

	// ...........computation parameter....................//
	int locTaskId, locTaskIdx;
	double disThre = 2 - 2 * TopologyMain.thre;
	double cellEps = Math.sqrt(disThre);

	int recStreCnt = 0;

	// ..........................

	public void indexCellSwVec(String swvec, String swcompid, int celltype) {

		int len = swvec.length(), pre = 0;
		double tmpdim = 0.0;
		String cellstr = new String();
		int tmpcell = 0;

		ArrayList<Integer> tmpCellVec = new ArrayList<Integer>();
		ArrayList<Double> tmpSwVec = new ArrayList<Double>();
		Queue<String> tmpCellSw = new LinkedList<String>();

		for (int i = 0; i < len; ++i) {
			if (swvec.charAt(i) == ',') {

				tmpdim = Double.valueOf(swvec.substring(pre, i));
				tmpSwVec.add(tmpdim);

				if (tmpdim >= 0) {

					// it is floor here
					tmpcell = (int) Math.floor((double) tmpdim / cellEps);
					tmpCellVec.add(tmpcell);

					cellstr = cellstr + Integer.toString(tmpcell) + ",";

				} else {

					tmpcell = (int) (-1)
							* (int) Math.ceil((double) -1 * tmpdim / cellEps);
					tmpCellVec.add(tmpcell);

					cellstr = cellstr + Integer.toString(tmpcell) + ",";
				}

				pre = i + 1;
			}
		}

		if (swVec.containsKey(swcompid) == true) {

			return;

		} else {

			swVec.put(swcompid, tmpSwVec);
		}

		if (cellVec.containsKey(cellstr) == false) {
			cellVec.put(cellstr, tmpCellVec);
			tmpCellSw.add(swcompid);
			cellSw.put(cellstr, tmpCellSw);

			cellType.put(cellstr, celltype);

		} else {
			cellSw.get(cellstr).add(swcompid);
		}

		return;
	}

	public int cellTypeCal(String cellstr) {

		int taskid = 0, tmp = 0;

		List<String> list = new ArrayList<String>(Arrays.asList(cellstr
				.split(",")));
		int size = list.size();
		tmp = Integer.parseInt(list.get(0));

		taskid = (tmp > 0 ? 1 : 0);

		for (int j = 1; j < size; ++j) {

			tmp = Integer.parseInt(list.get(j));
			tmp = (tmp > 0 ? 2 : 1);
			taskid = (int) (taskid + (tmp - 1) * Math.pow(2, j));
		}

		return (taskid == locTaskIdx ? 1 : 0);
	}

	boolean correCalDisApprox(double disthre_sq, String cswid1, String cswid2) {

		double tmpdis = 0.0, tmpval = 0;
		ArrayList<Double> v1 = swVec.get(cswid1);
		ArrayList<Double> v2 = swVec.get(cswid1);

		int minsize = Math.min(v1.size(), v2.size());
		for (int i = 0; i < minsize; ++i) {
			tmpval = (v1.get(i) - v2.get(i));
			tmpdis += (tmpval * tmpval);
		}
		for (int i = minsize; i < v1.size(); ++i) {
			tmpval = v1.get(i);
			tmpdis += (tmpval * tmpval);
		}
		for (int i = minsize; i < v2.size(); ++i) {
			tmpval = v2.get(i);
			tmpdis += (tmpval * tmpval);
		}

		return tmpdis <= disthre_sq ? true : false;

	}

	boolean checkSW_timeinstant(String cswId) {
		String[] items = cswId.split(",");
		if (Double.parseDouble(items[0]) > ts - TopologyMain.lag)
			return false;
		else
			return true;

	}

	void cellIntraLeadlag(BasicOutputCollector collector) {

		String cswid = new String();
		int size = 0;
		String[] csw_arr1 = new String[2];
		String[] csw_arr2 = new String[2];

		for (Map.Entry<String, Queue<String>> entry : cellSw.entrySet()) {
			String cellstr = entry.getKey();
			Queue<String> cellsws = entry.getValue();

			size = cellsws.size();
			String[] swsArr = new String[size + 1];
			swsArr = (String[]) cellsws.toArray();

			for (int i = 0; i < size; ++i) {

				cswid = swsArr[i];
				csw_arr1 = cswid.split(",");

				if (checkSW_timeinstant(cswid) == true) {

					for (int j = i + 1; j < size; j++) {

						if (correCalDisApprox(disThre, swsArr[i], swsArr[j]) == true) {
							csw_arr2 = swsArr[j].split(",");

							collector.emit(new Values(ts, csw_arr1[1],
									csw_arr2[1], Double
											.parseDouble(csw_arr2[0])
											- Double.parseDouble(csw_arr1[1]),
									0)); // modification

							// "ts", "leader","follower", "lag", "corre"))

						}

					}
				} else {
					break;
				}

			}

		}
		return;
	}

	boolean cellInterCheck(String cellstr1, String cellstr2) {
		ArrayList<Integer> cellvec1 = cellVec.get(cellstr1);
		ArrayList<Integer> cellvec2 = cellVec.get(cellstr2);

		int minsize = Math.min(cellvec1.size(), cellvec2.size());

		for (int i = 0; i < minsize; ++i) {
			if (Math.abs((cellvec1.get(i) - cellvec2.get(i))) > 1)
				return false;
		}
		for (int i = minsize; i < cellvec1.size(); ++i) {
			if (Math.abs((cellvec1.get(i))) > 1)
				return false;
		}
		for (int i = minsize; i < cellvec2.size(); ++i) {
			if (Math.abs((cellvec2.get(i))) > 1)
				return false;
		}

		return true;
	}

	void cellInterLeadlag(BasicOutputCollector collector) {

		int size = 0;

		String[] csw_arr1 = new String[2];
		String[] csw_arr2 = new String[2];
		ArrayList<String> cellList = new ArrayList<String>();
		cellList.addAll(cellVec.keySet());
		size = cellList.size();

		Queue<String> cellsws1 = new LinkedList<String>();
		Queue<String> cellsws2 = new LinkedList<String>();

		boolean flag = false;

		for (int i = 0; i < size; i++) {
			for (int j = i + 1; j < size; ++j) {
				if (cellInterCheck(cellList.get(i), cellList.get(j)) == true) {

					cellsws1 = cellSw.get(cellList.get(i));
					cellsws2 = cellSw.get(cellList.get(j));

					flag = false;

					for (String csw_str1 : cellsws1) {
						csw_arr1 = csw_str1.split(",");

						if (Double.parseDouble(csw_arr1[0]) == ts
								- TopologyMain.lag) {

							flag = true;

							for (String csw_str2 : cellsws2) {

								if (correCalDisApprox(disThre, csw_str1,
										csw_str2) == true) {
									csw_arr2 = csw_str2.split(",");

									collector
											.emit(new Values(
													ts,
													csw_arr1[1],
													csw_arr2[1],
													Double.parseDouble(csw_arr2[0])
															- Double.parseDouble(csw_arr1[1]),
													0));

									// ("ts", "pair","lag","corre"));

								}

							}
						} else {
							break;
						}

					}

					if (flag == false) {
						for (String csw_str2 : cellsws2) {
							csw_arr2 = csw_str2.split(",");

							if (Double.parseDouble(csw_arr1[0]) == ts
									- TopologyMain.lag) {

								flag = true;

								for (String csw_str1 : cellsws1) {

									if (correCalDisApprox(disThre, csw_str1,
											csw_str2) == true) {
										csw_arr1 = csw_str1.split(",");

										collector
												.emit(new Values(
														ts,
														csw_arr2[1],
														csw_arr1[1],
														Double.parseDouble(csw_arr1[0])
																- Double.parseDouble(csw_arr2[1]),
														0));

										// ("ts", "pair","lag","corre"));

									}

								}
							} else {
								break;
							}

						}
					}

				}
			}
		}

		return;

	}

	void cellLeadlagCorre(BasicOutputCollector collector) {

		cellIntraLeadlag(collector);
		cellInterLeadlag(collector);

	}

	public void localIdxRenew() {
		//
		// cellIdx.clear();
		// streIdx.clear();
		// cellStre.clear();
		return;
	}

	@Override
	public void cleanup() {

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

		declarer.declare(new Fields("ts", "leader", "follower", "lag", "corre"));
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

		// declarer.declareStream("dataTup", new Fields("id", "strevec", "ts",
		// "celltype"));
		// declarer.declareStream("calCommand", new Fields("command",
		// "taskid"));

		streType = input.getSourceStreamId();

		if (streType.compareTo("dataTup") == 0) {

			recStreCnt++;

			ts = input.getDoubleByField("ts");
			String swStr = input.getStringByField("strevec");
			int swId = input.getIntegerByField("id");
			celltype = input.getIntegerByField("celltype");

			indexCellSwVec(swStr, Double.toString(ts) + Integer.toString(swId),
					celltype);

		} else if (streType.compareTo("calCommand") == 0) {

			commandStr = input.getStringByField("command");
			preTaskId = input.getLongByField("taskid");

			preTaskIdx.add(preTaskId);
			if (preTaskIdx.size() < TopologyMain.preBoltNum) {
				return;
			}

			cellLeadlagCorre(collector);

			localIdxRenew();

			curtstamp = ts + 1;
			preTaskIdx.clear();
		}

		return;
	}
}
