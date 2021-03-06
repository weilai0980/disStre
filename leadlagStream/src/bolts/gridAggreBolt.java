package bolts;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;

import main.TopologyMain;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class gridAggreBolt extends BaseBasicBolt {

	double curtstamp = TopologyMain.winSize - 1;

	HashSet<String> pairDirec = new HashSet<String>();


	
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
		String pairstr = input.getStringByField("pair");

		if (ts > curtstamp) {

			curtstamp = ts;
			pairDirec.clear();

			pairDirec.add(pairstr);

		} else if (ts < curtstamp) {
			System.out
					.printf("!!!!!!!!!!!!! direct AjustAggreBolt time sequence disorder\n");
		} else if (Math.abs(ts - curtstamp) <= 1e-3) {

			pairDirec.add(pairstr);

		}

		return;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
	}

}
