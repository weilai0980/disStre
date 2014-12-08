package bolts;

import main.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class partCalBolt extends BaseBasicBolt {

	double curtstamp = TopologyMain.winSize - 1;

	String[] vecIdx = new String[TopologyMain.nstream + 10];
	Double[][] vecData = new Double[TopologyMain.nstream + 10][TopologyMain.winSize + 10];

	int[] strid = new int[TopologyMain.nstream + 10];
	int[] strlocal = new int[TopologyMain.nstream + 10];
	HashMap<Integer, Integer> stridMap = new HashMap<Integer, Integer>();
	int stridcnt = 0;

	HashMap<String, Integer> gridIdx = new HashMap<String, Integer>();
	int gridIdxcnt = 0;

	List<List<Integer>> gridStre = new ArrayList<List<Integer>>(
			TopologyMain.gridIdxN + 5);

	ArrayList<String> qualPair = new ArrayList<String>();

	public void vecBatchAna() {

		int len = 0, cnt = 0, pre = 0;
		String tmp = new String();
		for (int j = 0; j < stridcnt; ++j) {
			len = vecIdx[j].length();
			tmp = vecIdx[j];
			cnt = 0;
			pre = 0;
			for (int i = 0; i < len; ++i) {
				if (tmp.charAt(i) == ',') {
					vecData[j][cnt++] = Double.valueOf(tmp.substring(pre, i));
					pre = i + 1;
				}
			}
		}

		return;
	}

	public int vecAna(String orgstr, double vecval[]) {

		int len = orgstr.length();
		int cnt = 0, pre = 0;
		for (int i = 0; i < len; ++i) {
			if (orgstr.charAt(i) == ',') {
				vecval[cnt++] = Double.valueOf(orgstr.substring(pre, i));
				pre = i + 1;
			}
		}
		return cnt;
	}

	public double deviCal(double vec[], double exp, int l) {
		double sum = 0.0;
		for (int i = 0; i < l; ++i) {
			sum += ((vec[i] - exp) * (vec[i] - exp));
		}

		return sum / l;
	}

	public double expCal(double vec[], int l) {
		double sum = 0.0;
		for (int i = 0; i < l; ++i) {
			sum += vec[i];
		}

		return (double) sum / l;
	}

	int correCalDis(double thre, int streid1, int streid2, int len) {
		double dis = 0.0, tmp = 0.0;

		for (int i = 0; i < len; ++i) {
			tmp += ((vecData[streid1][i] - vecData[streid2][i]) * (vecData[streid1][i] - vecData[streid2][i]));
		}
		dis = tmp;

		return (dis <= 2 - 2 * thre) ? 1 : 0;
	}

	public int localStreIdx(int hostid, String vecdata) {
		int i = 0;

		if (stridMap.containsKey(hostid) == true) {
			i = stridMap.get(hostid);
		} else {
			stridMap.put(hostid, stridcnt);
			strid[stridcnt] = hostid;
			i = stridcnt++;
		}

		vecIdx[i] = vecdata;
		return i;
	}

	public int localGridIdx(String grid) {

		if (gridIdx.containsKey(grid) == true) {
			return gridIdx.get(grid);
		} else {
			gridIdx.put(grid, gridIdxcnt);
			gridIdxcnt++;

			gridStre.add(new ArrayList<Integer>());
			return gridIdxcnt - 1;
		}
	}

	public void localGridStreIdx(String grid, int hostid, String vecdata,
			int locFlag) {
		int streNo = localStreIdx(hostid, vecdata);
		int gridNo = localGridIdx(grid);

		strlocal[streNo] = locFlag;
		gridStre.get(gridNo).add(streNo);

		return;
	}

	public void localIdxRenew() {

		stridcnt = 0;
		stridMap.clear();

		gridIdx.clear();
		gridIdxcnt = 0;

		gridStre.clear();
		qualPair.clear();

		return;
	}

	public int correGrid(int gridNo, double thre,
			BasicOutputCollector collector, ArrayList<String> resPair) {

		int rescnt = 0, i = 0, j = 0, tmpcnt = gridStre.get(gridNo).size();
		int stre1 = 0, stre2 = 0;

		String Pair = new String();

		for (i = 0; i < tmpcnt; ++i) {

			stre1 = gridStre.get(gridNo).get(i);

			for (j = i + 1; j < tmpcnt; ++j) {

				stre2 = gridStre.get(gridNo).get(j);

				// if (strlocal[stre1] == 0 && strlocal[stre2] == 0) {
				// } else {

				if ((strlocal[stre1] == 1 && strlocal[stre2] == 0)
						|| (strlocal[stre1] == 0 && strlocal[stre2] == 1)) {

					if (correCalDis(thre, stre1, stre2, TopologyMain.winSize) == 1) {

						if (strid[stre1] > strid[stre2]) {

							Pair = (Integer.toString(strid[stre2]) + "," + Integer
									.toString(strid[stre1]));

							rescnt++;
							resPair.add(Pair);

						} else {

							Pair = (Integer.toString(strid[stre1]) + "," + Integer
									.toString(strid[stre2]));

							rescnt++;
							resPair.add(Pair);

						}
					}
				}

			}
		}

		return rescnt;
	}

	@Override
	public void cleanup() {
		// System.out.println("-- Word Counter [" + name + "-" + id + "] --");
		// for (Map.Entry<String, Integer> entry : counters.entrySet()) {
		// System.out.println(entry.getKey() + ": " + entry.getValue());
		// }

		// ....output stream........

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

		declarer.declare(new Fields("ts", "pair"));

	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {

		return;
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub

		double ts = input.getDoubleByField("ts");
		String coordstr = input.getStringByField("coord");
		String vecstr = input.getStringByField("vec");
		int hostid = input.getIntegerByField("hostid");
		int lflag = input.getIntegerByField("lflag");

		if (ts > curtstamp) {

			vecBatchAna();

			for (int i = 0; i < gridIdxcnt; ++i) {
				qualPair.clear();
				correGrid(i, TopologyMain.thre, collector, qualPair);

				Iterator<String> it = qualPair.iterator();
				String tmp = new String();
				while (it.hasNext()) {
					tmp = it.next();
					collector.emit(new Values(curtstamp, tmp));
				}
			}

			localIdxRenew();
			localGridStreIdx(coordstr, hostid, vecstr, lflag);
			curtstamp = ts;

		} else if (ts < curtstamp) {
			System.out.printf("!!!!!!!!!!!!! time sequence disorder\n");
		} else if (Math.abs(ts - curtstamp) <= 1e-3) {

			localGridStreIdx(coordstr, hostid, vecstr, lflag);

		}
		return;
	}
}
