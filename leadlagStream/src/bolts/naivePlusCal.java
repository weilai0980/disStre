package bolts;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import main.TopologyMain;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class naivePlusCal extends BaseBasicBolt {

	// .......temporal order management.............

	double curtstamp = TopologyMain.winSize - 1;

	// ..........memory managment..........

	List<List<Integer>> strpair = new ArrayList<List<Integer>>(
			TopologyMain.nstreBolt + 5);

	HashMap<Integer, Integer> strid = new HashMap<Integer, Integer>();
	int[] strIdxId = new int[TopologyMain.nstreBolt + 5];
	ArrayList<Integer> idMap = new ArrayList<Integer>();
	int stridcnt = 0;

	String[] strvec = new String[TopologyMain.nstreBolt + 5];
	short[] strvecflag = new short[TopologyMain.nstreBolt + 5];

	int rescnt = 0;

	String streType = new String(), commandStr = new String();
	long preTaskId = 0;
	double ts = 0;
	HashSet<Long> preTaskIdx = new HashSet<Long>();
	String[] blockPair_str = new String[3];
	String vecstr = new String();
	int hostBlock = 0;

	// ...........new added variables for memory management..........

	public double[][] strevec = new double[TopologyMain.nstreBolt + 10][TopologyMain.winSize + 10];
	public int[] vecst = new int[TopologyMain.nstreBolt + 10];
	public int[] veced = new int[TopologyMain.nstreBolt + 10];
	public int queueLen = TopologyMain.winSize + 10;

	int lblock = 0, rblock = 0;
	ArrayList<Integer> block1 = new ArrayList<Integer>();
	ArrayList<Integer> block2 = new ArrayList<Integer>();

	int vecstrAna(String orgstr, double vecval[]) {

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

	void tspairStrAna(String orgstr, int ts[]) {
		int l = orgstr.length();
		for (int i = 0; i < l; ++i) {
			if (orgstr.charAt(i) == ',') {
				ts[0] = Integer.valueOf(orgstr.substring(0, i));
				ts[1] = Integer.valueOf(orgstr.substring(i + 1, l));
				return;
			}
		}
		return;
	}

	double deviCal(double vec[], double exp, int l) {
		double sum = 0.0;
		for (int i = 0; i < l; ++i) {
			sum += ((vec[i] - exp) * (vec[i] - exp));
		}
		return sum / l;
	}

	double expCal(double vec[], int l) {
		double sum = 0.0;
		for (int i = 0; i < l; ++i) {
			sum += vec[i];
		}
		return (double) sum / l;
	}

	int correCal(String str1, String str2, double thre, int strid1, int strid2,
			double val[]) {

		double cor = 0.0, tmp = 0.0;
		double[] vec1 = new double[TopologyMain.winSize + 10];
		double[] vec2 = new double[TopologyMain.winSize + 10];
		int len = vecstrAna(str1, vec1);
		vecstrAna(str2, vec2);

		double exp1 = expCal(vec1, len);
		double exp2 = expCal(vec2, len);

		double dev1 = deviCal(vec1, exp1, len);
		double dev2 = deviCal(vec2, exp2, len);

		if (Math.abs(dev1 - 0.0) < 1e-6 || Math.abs(dev2 - 0.0) < 1e-6) {
			return 0;
		}

		for (int i = 0; i < len; ++i) {
			tmp += ((vec1[i] - exp1) * (vec2[i] - exp2));
		}

		cor = tmp / (Math.sqrt(dev1) * Math.sqrt(dev2) * len);

		val[0] = cor;

		return (cor >= thre) ? 1 : 0;

	}

	void localIdx(int lstre, int rstre, int hoststre, String vecdata) {
		int lno = 0, rno = 0;

		if (strid.containsKey(lstre) == true) {
			lno = strid.get(lstre);
		} else {
			strpair.add(new ArrayList<Integer>());
			strid.put(lstre, stridcnt);
			lno = stridcnt;

			idMap.add(lno, lstre);
			strIdxId[lno] = lstre;

			stridcnt++;
		}

		if (strid.containsKey(rstre) == true) {
			rno = strid.get(rstre);
		} else {
			strpair.add(new ArrayList<Integer>());
			strid.put(rstre, stridcnt);
			rno = stridcnt;
			idMap.add(rno, rstre);
			strIdxId[rno] = rstre;

			stridcnt++;
		}

		if (lstre < rstre) {
			strpair.get(lno).add(rno);
		} else {
			strpair.get(rno).add(lno);
		}

		if (lstre == hoststre) {

			strvec[lno] = vecdata;
			strvecflag[lno] = 1;

		} else {
			strvec[rno] = vecdata;
			strvecflag[rno] = 1;

		}
		return;
	}

	int indexSw(int swId, String vecdata) {
		int idx = 0;

		if (strid.containsKey(swId) == true) {
			idx = strid.get(swId);
		} else {
			strpair.add(new ArrayList<Integer>());

			idx = strid.size();
			strid.put(swId, idx);

		}

		strevec[idx][veced[idx]] = Double.parseDouble(vecdata);
		veced[idx] = (veced[idx] + 1) % queueLen;

		return idx;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

		declarer.declare(new Fields("ts", "pair"));
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub

		streType = input.getSourceStreamId();

		if (streType.compareTo("streamData") == 0) {

			// new Fields("ts", "blockPair","hostBlock", "id", "vec"));

			ts = input.getDoubleByField("ts");
			blockPair_str = input.getStringByField("blockPair").split(",");
			hostBlock = input.getIntegerByField("hostBlock");
			vecstr = input.getStringByField("vec");
			int swId = input.getIntegerByField("id");

			// tspairStrAna(tsstr, tspair);

			lblock = Integer.parseInt(blockPair_str[0]);
			rblock = Integer.parseInt(blockPair_str[1]);

			if (lblock == hostBlock) {
				block1.add(indexSw(swId, vecstr));
			} else if (rblock == hostBlock) {
				block2.add(indexSw(swId, vecstr));

			}

		} else if (streType.compareTo("calCommand") == 0) {
			double tmpval = 0.0;
			double[] cval = new double[4];

			curtstamp = ts;

			commandStr = input.getStringByField("command");
			preTaskId = input.getIntegerByField("taskid");

			preTaskIdx.add(preTaskId);
			if (preTaskIdx.size() < TopologyMain.preBoltNum) {
				return;
			}

			// ..........test.............
			// System.out.printf("%f %d\n", curtstamp, stridcnt);
			// .........................

			for (int i = 0; i < stridcnt; ++i) {

				for (Integer j : strpair.get(i)) {

					if ((strvecflag[i] == 1 && strvecflag[j] == 1)
							&& (tmpval = correCal(strvec[i], strvec[j],
									TopologyMain.thre, idMap.get(i),
									idMap.get(j), cval)) == 1) {
						rescnt++;

						if (strIdxId[i] < strIdxId[j]) {
							collector.emit(new Values(curtstamp, Integer
									.toString(strIdxId[i])
									+ ","
									+ Integer.toString(strIdxId[j])
									+ ","
									+ Double.toString(cval[0])));
						} else {
							collector.emit(new Values(curtstamp, Integer
									.toString(strIdxId[j])
									+ ","
									+ Integer.toString(strIdxId[i])
									+ ","
									+ Double.toString(cval[0])));
						}
					}
				}
			}

			strpair.clear();
			strid.clear();
			idMap.clear();
			stridcnt = 0;
			for (int i = 0; i < TopologyMain.nstreBolt + 5; ++i) {
				strvecflag[i] = 0;
			}
			preTaskIdx.clear();

		}
		return;

	}

	@Override
	public void cleanup() {

		return;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {

		for (int i = 0; i < TopologyMain.nstreBolt + 5; ++i) {
			strvecflag[i] = 0;
		}

		return;
	}

}
