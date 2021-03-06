package bolts;

import main.*;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class naivePreBolt extends BaseBasicBolt {


//	public double curtstamp = TopologyMain.winSize-1;
	
	public double curtstamp = 0.0;
	public double ststamp = 0.0;
	
	public int taskId = 0;

	public double[][] strevec = new double[TopologyMain.nstreBolt + 10][TopologyMain.winSize+10];
	public int[] vecst = new int[TopologyMain.nstreBolt + 10];
	public int[] veced = new int[TopologyMain.nstreBolt + 10];
    public int queueLen=TopologyMain.winSize+10;
	
	public int[] vecflag = new int[TopologyMain.nstreBolt + 10];

	public int[] streid = new int[TopologyMain.nstreBolt + 10];
	public int streidCnt = 0;

//	public int flag = 1;

	/**
	 * At the end of the spout (when the cluster is shutdown We will show the
	 * word counters
	 */
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
		declarer.declare(new Fields("ts", "tspair", "host", "vec"));
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		// TODO Auto-generated method stub

		for (int j = 0; j < TopologyMain.nstreBolt + 10; j++) {
			vecst[j] = 0;
			veced[j] = 0;
//			veced[j] = TopologyMain.winSize-1;

			vecflag[j] = 0;
			streid[j] = 0;

//			tmpstrcnt[j] = 0;
		}

	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub

		double ts = input.getDoubleByField("ts");
		double tmpval = input.getDoubleByField("value");
		int sn = input.getIntegerByField("sn");
		int i = 0, tmpsn = 0;
		
		int veccnt=0;

		if (ts > curtstamp) {

			String tspairstr = new String();

			if (ts - ststamp >= TopologyMain.winSize) {
				ststamp++;

				int tmpstrid = 0, k = 0;
				String vecstr = new String();

				for (int j = 0; j < streidCnt; ++j) {
					tmpstrid = streid[j];

					// ...........................//
					vecstr = "";		
					veccnt=0;
					
					k = vecst[j];
					while (k != veced[j]) {
						vecstr = vecstr + Double.toString(strevec[j][k]) + ",";
						k = (k + 1) % queueLen;
						
						veccnt++;
					}
					
					for (k = 0; k < tmpstrid; ++k) {

						tspairstr = Integer.toString(k) + ","
								+ Integer.toString(tmpstrid);


						collector.emit(new Values(curtstamp, tspairstr, tmpstrid,
								vecstr));

						
						
						
					}
					for (k = tmpstrid + 1; k < TopologyMain.nstream; ++k) {

						tspairstr = Integer.toString(tmpstrid) + ","
								+ Integer.toString(k);
						
						
					

						collector.emit(new Values(curtstamp, tspairstr, tmpstrid,
								vecstr));
					}

				}

				for (int j = 0; j < TopologyMain.nstreBolt + 5; ++j) {
					vecst[j] = (vecst[j] + 1)%queueLen;
				}

			}

			// ...........prepare for next timestamp tuple...............//

			curtstamp = ts;

			for (int j = 0; j < TopologyMain.nstreBolt + 5; ++j) {
				vecflag[j] = 0;
			}

			streidCnt=0;
			
			
			// .........................................................//
			for (i = 0; i < streidCnt; ++i) {
				if (streid[i] == sn) {
					tmpsn = i;
					break;
				}
			}
			if (i == streidCnt) {
				streid[i] = sn;
				tmpsn = streidCnt;
				streidCnt++;

			}
			if (vecflag[tmpsn] == 0) {

				strevec[tmpsn][veced[tmpsn]] = tmpval;
				veced[tmpsn] = (veced[tmpsn] + 1) % queueLen;

				vecflag[tmpsn] = 1;
			}

		} else if (ts < curtstamp) {
			System.out.printf("!!!!!!!!!!!!! time sequence disorder\n");
		} else if (Math.abs(ts - curtstamp) <= 1e-3) {

			for (i = 0; i < streidCnt; ++i) {
				if (streid[i] == sn) {
					tmpsn = i;
					break;
				}
			}
			if (i == streidCnt) {
				streid[i] = sn;
				tmpsn = streidCnt;
				streidCnt++;

			}
			if (vecflag[tmpsn] == 0) {
				strevec[tmpsn][veced[tmpsn]] = tmpval;
				veced[tmpsn] = (veced[tmpsn] + 1) % queueLen;

				vecflag[tmpsn] = 1;
			}

		}

	}

}
