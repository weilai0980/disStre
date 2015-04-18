package bolts;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class testGroupBolt extends BaseBasicBolt {
	
	public int tupcnt=0;

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub
		
		double ts = input.getDoubleByField("ts");
//		double tmpval = input.getDoubleByField("value");
//		int sn = input.getIntegerByField("sn");
		
		tupcnt++;
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
		
		
		
	}

}
