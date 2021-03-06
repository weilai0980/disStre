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

	// public double curtstamp = TopologyMain.winSize-1;

	public double curtstamp = 0.0;
	public double ststamp = 0.0;

	public int taskId = 0;

	public double[][] strevec = new double[TopologyMain.nstreBolt + 10][TopologyMain.winSize + 10];
	public int[] vecst = new int[TopologyMain.nstreBolt + 10];
	public int[] veced = new int[TopologyMain.nstreBolt + 10];
	public int queueLen = TopologyMain.winSize + 10;

	public int[] vecflag = new int[TopologyMain.nstreBolt + 10];

	public int[] streid = new int[TopologyMain.nstreBolt + 10];
	public int streidCnt = 0;

	String streType = new String();
	double ts = 0.0;
	int localTaskId = 0;
	

	
	

	// ............custom metric............

	// transient CountMetric _contByte;

	void iniMetrics(TopologyContext context) {
		// _contByte= new CountMetric();
		//
		// context.registerMetric("emByte_count", _contByte, 5);

	}

	void updateMetrics(double val) {
		// _contByte.incrBy(val);
		return;
	}

	// .....................................

	@Override
	public void cleanup() {

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("streamData", new Fields("ts", "tspair", "host",
				"vec"));

		declarer.declareStream("calCommand", new Fields("command", "taskid"));

	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		// TODO Auto-generated method stub

		for (int j = 0; j < TopologyMain.nstreBolt + 10; j++) {
			vecst[j] = 0;
			veced[j] = 0;
			// veced[j] = TopologyMain.winSize-1;
			
			if(TopologyMain.iniWindow==0)
			{
				veced[j] = TopologyMain.winSize - 1;
			}	

			vecflag[j] = 0;
			streid[j] = 0;

		}

		localTaskId = context.getThisTaskId();

		iniMetrics(context);

	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub

		streType = input.getSourceStreamId();

		if (streType.compareTo("dataStre") == 0) {

			ts = input.getDoubleByField("ts");
			double tmpval = input.getDoubleByField("value");
			int sn = input.getIntegerByField("sn");
			int i = 0, tmpsn = 0;

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

		} else if (streType.compareTo("contrStre") == 0) {

			String tspairstr = new String();

			// System.out.printf("---------  got control stream \n");

			if (ts - ststamp >= TopologyMain.winSize - 1) {
				ststamp++;
				curtstamp = ts;

				// ..........test.............
				// System.out.printf("%f %d\n", curtstamp, streidCnt);
				// .........................

				int tmpstrid = 0, k = 0;
				String vecstr = new String();

				for (int j = 0; j < streidCnt; ++j) {

					tmpstrid = streid[j];
					vecstr = "";

					k = vecst[j];
					while (k != veced[j]) {
						vecstr = vecstr + Double.toString(strevec[j][k]) + ",";
						k = (k + 1) % queueLen;
					}

					for (k = 0; k < tmpstrid; ++k) {

						tspairstr = Integer.toString(k) + ","
								+ Integer.toString(tmpstrid);

						collector.emit("streamData", new Values(curtstamp,
								tspairstr, tmpstrid, vecstr));

					}
					for (k = tmpstrid + 1; k < TopologyMain.nstream; ++k) {

						tspairstr = Integer.toString(tmpstrid) + ","
								+ Integer.toString(k);

						collector.emit("streamData", new Values(curtstamp,
								tspairstr, tmpstrid, vecstr));
					}

				}

				for (int j = 0; j < TopologyMain.nstreBolt + 5; ++j) {
					vecst[j] = (vecst[j] + 1) % queueLen;
				}

				// .......... custom metrics........
				updateMetrics(streidCnt * TopologyMain.winSize);

			}

			collector.emit("calCommand", new Values(Double.toString(ts),
					localTaskId));

			// ...........prepare for next timestamp tuple...............//

			for (int j = 0; j < TopologyMain.nstreBolt + 5; ++j) {
				vecflag[j] = 0;
			}

			streidCnt = 0;

			// ........................................................//

		}
	}
}
