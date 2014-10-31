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

public class rpCalBolt extends BaseBasicBolt {

	// ............input time order..............//

	double curtstamp = TopologyMain.winSize - 1;
	String streType = new String();
	double ts = 0.0;

	String commandStr = new String();
	long preTaskId = 0;

	HashSet<Long> preTaskIdx = new HashSet<Long>();
	// .........memory management for sliding windows.................//

	Double[][] streamVec = new Double[TopologyMain.nstreBolt + 10][TopologyMain.winSize + 10];

	HashMap<Integer, Integer> streIdx = new HashMap<Integer, Integer>();
	HashMap<String, Integer> bucketIdx = new HashMap<String, Integer>();

	List<List<Integer>> bucketStre = new ArrayList<List<Integer>>(
			TopologyMain.gridIdxN + 5);

	ArrayList<String> resPair = new ArrayList<String>();

	// ...........computation parameter....................//
	int locTaskId;
	double disThre = 2 - 2 * TopologyMain.thre;
	final double disThreRoot = Math.sqrt(disThre);
	double cellEps = Math.sqrt(disThre);

	// ........................................................................//

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

	public void IndexingStre(int streid, String streamStr, String bucket) {

		if (streIdx.containsKey(streid) == true) {
			return;
		}

		int streNo = streIdx.size(), bucketNo = bucketIdx.size();
		streIdx.put(streid, streNo);

		if (bucketIdx.containsKey(bucket) == true) {

			bucketStre.get(bucketIdx.get(bucket)).add(streid);

		} else {

			bucketIdx.put(bucket, bucketNo);
			bucketStre.add(new ArrayList<Integer>());

			bucketStre.get(bucketIdx.get(bucket)).add(streid);
		}

		vecAna(streamVec, streNo, streamStr);

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

	void bucketCorrCal(double threshold) {

		resPair.clear();
		int bucketcnt = bucketIdx.size(), streamCnt = 0;

		for (int i = 0; i < bucketcnt; ++i) {

			streamCnt = bucketStre.get(i).size();

			for (int j = 0; j < streamCnt; ++j) {
				for (int k = j + 1; k < streamCnt; ++k) {

					if (correCalDisReal(threshold, bucketStre.get(i).get(j),
							bucketStre.get(i).get(k)) == 1) {
						resPair.add(Integer.toString(bucketStre.get(i).get(j))
								+ ","
								+ Integer.toString(bucketStre.get(i).get(k)));

					}
				}
			}

		}
		return;
	}

	public void localIdxRenew() {

		bucketIdx.clear();
		streIdx.clear();

		bucketStre.clear();
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
		// "bucket", "strevec"));

		streType = input.getSourceStreamId();

		if (streType.compareTo("streamData") == 0) {

			ts = input.getDoubleByField("ts");
			String strestr = input.getStringByField("strevec");
			String bucket = input.getStringByField("bucket");
			int streid = input.getIntegerByField("streId");

			if (Math.abs(curtstamp - ts) <= 1e-3) {
				IndexingStre(streid, strestr, bucket);
			}

		} else if (streType.compareTo("calCommand") == 0) {

			commandStr = input.getStringByField("command");
			preTaskId = input.getLongByField("taskid");

			preTaskIdx.add(preTaskId);
			if (preTaskIdx.size() < TopologyMain.preBoltNum) {
				return;
			}

			bucketCorrCal(disThreRoot);

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
