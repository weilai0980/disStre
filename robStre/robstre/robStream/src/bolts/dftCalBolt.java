package bolts;

import main.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class dftCalBolt extends BaseBasicBolt {

	// ............input time order..............//

	double curtstamp = TopologyMain.winSize - 1;
	String streType = new String();
	double ts = 0.0;

	String commandStr = new String();
	long preTaskId = 0;

	HashSet<Long> preTaskIdx = new HashSet<Long>();
	// .........memory management for sliding windows.................//

	Double[][] streamVec = new Double[TopologyMain.nstreBolt + 10][TopologyMain.winSize + 10];
	Double[][] dftVec = new Double[TopologyMain.nstreBolt + 10][TopologyMain.winSize + 10];
	int[][] cellVec = new int[TopologyMain.gridIdxN + 5][TopologyMain.winSize + 5];

	HashMap<Integer, Integer> streIdx = new HashMap<Integer, Integer>();
	HashMap<String, Integer> cellIdx = new HashMap<String, Integer>();

	List<List<Integer>> cellHostStre = new ArrayList<List<Integer>>(
			TopologyMain.gridIdxN + 5);
	List<List<Integer>> cellNbStre = new ArrayList<List<Integer>>(
			TopologyMain.gridIdxN + 5);

	ArrayList<String> resPair = new ArrayList<String>();

	// ...........computation parameter....................//
	int locTaskId;
	double disThre = 2 - 2 * TopologyMain.thre;
	double cellEps = Math.sqrt(disThre);
	

	public void vecAna(Double mem[][], int idx, String vecstr) {
		int len = vecstr.length(), pre = 0, cnt = 0;
		double tmpval = 0.0;

		for (int i = 0; i < len; ++i) {
			if (vecstr.charAt(i) == ',') {

				tmpval = Double.valueOf(vecstr.substring(pre, i));
				mem[idx][cnt++] = tmpval;

			}
		}

		return;
	}

	public void vecAnaInt(int mem[][], int idx, String vecstr) {
		int len = vecstr.length(), pre = 0, cnt = 0, tmpval = 0;

		for (int i = 0; i < len; ++i) {
			if (vecstr.charAt(i) == ',') {

				tmpval = Integer.valueOf(vecstr.substring(pre, i));
				mem[idx][cnt++] = tmpval;

			}
		}

		return;
	}

	public void IndexingStre(int streid, String streamStr, String dftStr,
			String cellStr, int flag) {

		if (streIdx.containsKey(streid) == true) {
			return;
		}

		int streNo = streIdx.size(), cellNo = cellIdx.size();
		streIdx.put(streid, streNo);

		if (cellIdx.containsKey(cellStr) == true) {

			if (flag == 1) {
				cellHostStre.get(cellIdx.get(cellStr)).add(streid);
			} else {
				cellNbStre.get(cellIdx.get(cellStr)).add(streid);
			}
		} else {

			cellIdx.put(cellStr, cellNo);
			cellHostStre.add(new ArrayList<Integer>());
			cellNbStre.add(new ArrayList<Integer>());

			if (flag == 1) {
				cellHostStre.get(cellIdx.get(cellStr)).add(streid);
			} else {
				cellNbStre.get(cellIdx.get(cellStr)).add(streid);
			}
		}

		vecAna(streamVec, streNo, streamStr);
		vecAna(dftVec, streNo, dftStr);
		vecAnaInt(cellVec, cellNo, cellStr);

		return;
	}

	int correCalDisReal(double thre, int streid1, int streid2) {

		int memidx1 = streIdx.get(streid1), memidx2 = streIdx.get(streid2), k = 0;

		double tmpres = 0.0;

		for (k = 0; k < TopologyMain.winSize; ++k) {
			tmpres = tmpres + (streamVec[memidx1][k] - streamVec[memidx2][k])
					* (streamVec[memidx1][k] - streamVec[memidx2][k]);
		}

		return tmpres <= thre ? 1 : 0;

	}

	int correCalDisDFT(double thre, int streid1, int streid2) {

		int memidx1 = streIdx.get(streid1), memidx2 = streIdx.get(streid2), k = 0;

		double tmpres = 0.0;

		for (k = 0; k < TopologyMain.winSize; ++k) {
			tmpres = tmpres + (dftVec[memidx1][k] - dftVec[memidx2][k])
					* (dftVec[memidx1][k] - dftVec[memidx2][k]);
		}

		return tmpres <= thre ? 1 : 0;

	}

	void cellWithinCal(int cellidx, ArrayList<String> res) {

		int stre1 = 0, stre2 = 0;
		for (int i = 0; i < cellHostStre.get(cellidx).size(); i++) {
			for (int j = i + 1; j < cellHostStre.get(cellidx).size(); j++) {

				stre1 = cellHostStre.get(cellidx).get(i);
				stre2 = cellHostStre.get(cellidx).get(j);

				if (correCalDisDFT(2 - 2 * TopologyMain.thre, stre1, stre2) == 1) {

					// if (correCalDisReal(2 - 2 * TopologyMain.thre, stre1,
					// stre2) == 1)
					{

						if (stre1 > stre2)
							res.add(Integer.toString(stre2) + ","
									+ Integer.toString(stre1));
						else
							res.add(Integer.toString(stre1) + ","
									+ Integer.toString(stre2));
					}
				}

			}
		}

		for (int i = 0; i < cellHostStre.get(cellidx).size(); i++) {
			for (int j = 0; j < cellNbStre.get(cellidx).size(); j++) {

				stre1 = cellHostStre.get(cellidx).get(i);
				stre2 = cellNbStre.get(cellidx).get(j);

				if (correCalDisDFT(TopologyMain.thre, stre1, stre2) == 1) {

					// if (correCalDisReal(2 - 2 * TopologyMain.thre, stre1,
					// stre2) == 1)
					{

						if (stre1 > stre2)
							res.add(Integer.toString(stre2) + ","
									+ Integer.toString(stre1));
						else
							res.add(Integer.toString(stre1) + ","
									+ Integer.toString(stre2));
					}
				}

			}
		}

		return;
	}

	void cellCorrCal() {

		resPair.clear();
		int cellcnt = cellIdx.size();

		for (int i = 0; i < cellcnt; ++i) {

			cellWithinCal(i, resPair);
		}
		return;
	}

	public void localIdxRenew() {

		cellIdx.clear();
		streIdx.clear();

		cellHostStre.clear();
		cellNbStre.clear();
		resPair.clear();
		
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

		locTaskId = context.getThisTaskIndex();

		return;
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub

		// declarer.declareStream("streamData", new Fields("ts", "streId",
		// "cellCoor", "strevec","dftvec", "hostFlag"));
		//
		// declarer.declareStream("calCommand", new Fields("command",
		// "taskid"));

		streType = input.getSourceStreamId();

		if (streType.compareTo("streamData") == 0) {

			ts = input.getDoubleByField("ts");
			String strestr = input.getStringByField("strevec");
			String dftstr = input.getStringByField("dftvec");
			String cellstr = input.getStringByField("cellCoor");
			int streid = input.getIntegerByField("streId");
			int hostflag = input.getIntegerByField("hostFlag");

			if (Math.abs(curtstamp - ts) <= 1e-3) {
				IndexingStre(streid, strestr, dftstr, cellstr, hostflag);
			}

		} else if (streType.compareTo("calCommand") == 0) {

			commandStr = input.getStringByField("command");
			preTaskId = input.getLongByField("taskid");

			preTaskIdx.add(preTaskId);
			if (preTaskIdx.size() < TopologyMain.preBoltNum) {
				return;
			}

			cellCorrCal();

			for (String pair : resPair) {
				collector.emit(new Values(curtstamp, pair));
			}
			localIdxRenew();

			curtstamp = ts + 1;
			preTaskIdx.clear();
		}

		return;
	}
}