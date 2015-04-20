package bolts;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import main.TopologyMain;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class llcorrAgg extends BaseBasicBolt {

	double curtstamp = TopologyMain.winSize - 1;
	HashMap<String, HashSet<String>> followerLags_set = new HashMap<String, HashSet<String>>();

	@Override
	public void cleanup() {

		return;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {

		return;
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub

		double ts = input.getDoubleByField("ts");
		String leader_str = input.getStringByField("leader");

		String follower_str = input.getStringByField("follwer");
		String lag_str = input.getStringByField("lag");
		String follwerLag = leader_str + "," + follower_str + "," + lag_str;
		Double corre = input.getDoubleByField("corre");

		if (ts > curtstamp) {

			curtstamp = ts;
			followerLags_set.clear();

			if (followerLags_set.containsKey(leader_str) == true) {
				followerLags_set.get(leader_str).add(follwerLag);
			} else {
				followerLags_set.put(leader_str, new HashSet<String>());
				followerLags_set.get(leader_str).add(follwerLag);

			}

		} else if (ts < curtstamp) {
			System.out
					.printf("!!!!!!!!!!!!! direct AjustAggreBolt time sequence disorder\n");
		} else if (Math.abs(ts - curtstamp) <= 1e-3) {

			if (followerLags_set.containsKey(leader_str) == true) {
				followerLags_set.get(leader_str).add(follwerLag);
			} else {
				followerLags_set.put(leader_str, new HashSet<String>());
				followerLags_set.get(leader_str).add(follwerLag);

			}

		}

		return;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
	}

}
