//package bolts;
//
//public class indivStaComp {
//
//}
package bolts;

import java.util.HashMap;
import java.util.Map;

//import spouts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class indivStaComp extends BaseBasicBolt {

	// Integer id;
	// String name;
	// Map<String, Integer> counters;

	// ......stream processing.............//
	public static int winSize = 10;
	public double[] cycQueue = new double[winSize + 10];
	public int st = 0, ed = 0;

	public int nstream = 20000;
	public int nstreBolt = 10;

	public static int gtaskId = 0;

	public int taskId = 0;
	public double curtstamp = 0.0;

	// .....statistic measures.............//
	public double sum = 0.0;
	public double exp = 0.0;
	public double var = 0.0;
	public double[] vecvar = new double[winSize + 10];
	public long cnt = 0;
	public long precnt = 0;

	public int[] streid = new int[nstreBolt + 10];
	public int streidCnt = 0;

	public int[] tmpstrcnt = new int[100];

	int flag = 0;

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

	/**
	 * On create
	 */
//	@Override
//	public void prepare(Map stormConf, TopologyContext context) {
//		// this.counters = new HashMap<String, Integer>();
//		// this.name = context.getThisComponentId();
//		// this.id = context.getThisTaskId();
//
//		// ......stream processing.............//
//		// winSize = 10;
//		// cycQueue = new double[winSize + 10];
//
//		taskId = gtaskId;
//		gtaskId++;
//
//		st = 0;
//		ed = 0;
//
//		nstream = 20;
//
//		// .....statistic measures.............//
//		sum = 0.0;
//		exp = 0.0;
//		var = 0.0;
//		// public double[] vecvar = new double[winSize + 10];
//
//	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("pair", "priStre", "vecidx", "vecval"));
	}

//	@Override
//	public void execute(Tuple input, BasicOutputCollector collector) {
//
//		double ts = input.getDoubleByField("ts");
//		double tmpval = input.getDoubleByField("value");
//		int sn = input.getIntegerByField("sn");
//		// getByField("sn");
//		// int sn = (int) snfloat;
//		// String snstr = input.getStringByField("sn");
//
//		int i = 0;
//		// ................test.............................................//
//		cnt++;
//		System.out.printf("%d",sn);
////		tmpstrcnt[sn]++;
//
//		if (ts > curtstamp) {
//			curtstamp = ts;
//
//			// flag=1;
//			//
//			// if(precnt!=cnt)
//			// {
//			// System.out.printf("!!!!!!!!!!!!!!!!!!!! amount of data not consistent\n");
//			// }
//			// precnt=cnt;
//			// cnt=0;
//
//			System.out.printf("!!!!!!!!!!!!! task %d time stamp:   %d  %d",taskId,cnt,tmpstrcnt[sn]);
//
////			System.out.printf("\n");
////			for (int j = 0; j < 5; ++j) {
////
////				System.out.printf("%d  ", tmpstrcnt[j]);
////
////			}
//			System.out.printf("\n");
//
//			// System.out.printf("current time stamp:%f\n",curtstamp);
//		} else if (ts < curtstamp) {
//			System.out.printf("!!!!!!!!!!!!! time sequence   ");
//		} else if (Math.abs(ts - curtstamp) <= 1e-1) {
//
//			tmpstrcnt[sn]++;
//
//			// for(i=0;i<streidCnt;++i)
//			// {
//			// if(streid[i]==sn)
//			// {
//			// break;
//			// }
//			// }
//			//
//			//
//			// if(flag==1 && i==streidCnt)
//			// {
//			// System.out.printf("!!!!!!!!!!!!!!!!!!!! amount of time series not consistent\n");
//			// }
//			//
//			//
//			//
//			// if(i==streidCnt)
//			// {
//			// streid[streidCnt++]=sn;
//			// }
//
////			cnt++;
//		}
//		
//	}

	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		
		taskId = gtaskId;
		gtaskId++;

		st = 0;
		ed = 0;

		nstream = 20;

		// .....statistic measures.............//
		sum = 0.0;
		exp = 0.0;
		var = 0.0;
		// public double[] vecvar = new double[winSize + 10];
		
		
	}

//	@Override
//	public void execute(Tuple input) {
//		// TODO Auto-generated method stub
//		
//		double ts = input.getDoubleByField("ts");
//		double tmpval = input.getDoubleByField("value");
//		int sn = input.getIntegerByField("sn");
//		// getByField("sn");
//		// int sn = (int) snfloat;
//		// String snstr = input.getStringByField("sn");
//
//		int i = 0;
//		// ................test.............................................//
//		cnt++;
//		System.out.printf("%d",sn);
////		tmpstrcnt[sn]++;
//
//		if (ts > curtstamp) {
//			curtstamp = ts;
//
//			// flag=1;
//			//
//			// if(precnt!=cnt)
//			// {
//			// System.out.printf("!!!!!!!!!!!!!!!!!!!! amount of data not consistent\n");
//			// }
//			// precnt=cnt;
//			// cnt=0;
//
//			System.out.printf("!!!!!!!!!!!!! task %d time stamp:   %d  %d",taskId,cnt,tmpstrcnt[sn]);
//
////			System.out.printf("\n");
////			for (int j = 0; j < 5; ++j) {
////
////				System.out.printf("%d  ", tmpstrcnt[j]);
////
////			}
//			System.out.printf("\n");
//
//			// System.out.printf("current time stamp:%f\n",curtstamp);
//		} else if (ts < curtstamp) {
//			System.out.printf("!!!!!!!!!!!!! time sequence   ");
//		} else if (Math.abs(ts - curtstamp) <= 1e-1) {
//
//			tmpstrcnt[sn]++;
//
//			// for(i=0;i<streidCnt;++i)
//			// {
//			// if(streid[i]==sn)
//			// {
//			// break;
//			// }
//			// }
//			//
//			//
//			// if(flag==1 && i==streidCnt)
//			// {
//			// System.out.printf("!!!!!!!!!!!!!!!!!!!! amount of time series not consistent\n");
//			// }
//			//
//			//
//			//
//			// if(i==streidCnt)
//			// {
//			// streid[streidCnt++]=sn;
//			// }
//
////			cnt++;
//		}
//		
//	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub
		
		double ts = input.getDoubleByField("ts");
		double tmpval = input.getDoubleByField("value");
		int sn = input.getIntegerByField("sn");
		// getByField("sn");
		// int sn = (int) snfloat;
		// String snstr = input.getStringByField("sn");

		int i = 0;
		// ................test.............................................//
		cnt++;
		System.out.printf("%d",sn);
//		tmpstrcnt[sn]++;

		if (ts > curtstamp) {
			curtstamp = ts;

			// flag=1;
			//
			// if(precnt!=cnt)
			// {
			// System.out.printf("!!!!!!!!!!!!!!!!!!!! amount of data not consistent\n");
			// }
			// precnt=cnt;
			// cnt=0;

			System.out.printf("!!!!!!!!!!!!! task %d time stamp:   %d  %d",taskId,cnt,tmpstrcnt[sn]);

//			System.out.printf("\n");
//			for (int j = 0; j < 5; ++j) {
//
//				System.out.printf("%d  ", tmpstrcnt[j]);
//
//			}
			System.out.printf("\n");

			// System.out.printf("current time stamp:%f\n",curtstamp);
		} else if (ts < curtstamp) {
			System.out.printf("!!!!!!!!!!!!! time sequence   ");
		} else if (Math.abs(ts - curtstamp) <= 1e-1) {

			tmpstrcnt[sn]++;

			// for(i=0;i<streidCnt;++i)
			// {
			// if(streid[i]==sn)
			// {
			// break;
			// }
			// }
			//
			//
			// if(flag==1 && i==streidCnt)
			// {
			// System.out.printf("!!!!!!!!!!!!!!!!!!!! amount of time series not consistent\n");
			// }
			//
			//
			//
			// if(i==streidCnt)
			// {
			// streid[streidCnt++]=sn;
			// }

//			cnt++;
		}
		
	}
}