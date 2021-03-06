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

public class AdjustAggreBolt extends BaseBasicBolt {

	double curtstampInter = TopologyMain.winSize - 1;
	double curtstampDirec = TopologyMain.winSize - 1;
	double curt = TopologyMain.winSize - 1;

	int paircntDirec = 0, paircntInter = 0;

	HashSet<String> pairInter = new HashSet<String>();
	HashSet<String> pairDirec = new HashSet<String>();

	HashSet<String> pairRes = new HashSet<String>(),
			pairPreRes = new HashSet<String>();

	HashSet<String>[] pairs = new HashSet[3];
	int curset = 0;

	String streType = new String();

	public static int glAggBolt = 0;
	public int locAggBolt = 0;

	FileWriter fstream; // =new FileWriter("", true);
	BufferedWriter out; // = new BufferedWriter(fstream);

	@Override
	public void cleanup() {

		return;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {

		locAggBolt = glAggBolt++;

		for (int i = 0; i < 2; ++i) {
			pairs[i] = new HashSet<String>();

		}

//		try {
//			fstream = new FileWriter("apsRes.txt", false);
//			BufferedWriter out = new BufferedWriter(fstream);
//			// out.close();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}

		return;
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub

		double ts = input.getDoubleByField("ts");
		String pairstr = input.getStringByField("pair");

		streType = input.getSourceStreamId();

		//
		// if(ts==curt+2)
		// {
		//
		// }

		if (ts > curt + 1) {
			// ...........test............................
//			if (curt == 2) {
//				System.out.printf("AggreBolt  time stamp %f:   %d \n ", curt,
//						pairs[curset].size());
//			}

//			try {
//
//				fstream = new FileWriter("apsRes.txt", true);
//				BufferedWriter out = new BufferedWriter(fstream);
//
//				out.write("Timestamp  " + Double.toString(curt) + ", "
//						+ "total num  "
//						+ Integer.toString(pairs[curset].size()) + ": \n ");
//
//				for (String iter : pairs[curset]) {
//					out.write(iter + "\n");
//				}
//
//				out.write("\n");
//				out.close();
//
//			} catch (IOException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
			// .......................................

			curt = curt + 1;
			// pairRes.clear();
			// pairRes.add(pairstr);

			curset = 1 - curset;
			pairs[1 - curset].clear();
			pairs[1 - curset].add(pairstr);

		}

		else if (ts == curt + 1) {

			pairs[1 - curset].add(pairstr);

		} else if (ts == curt) {

			pairs[curset].add(pairstr);

			// pairRes.add(pairstr);
		}
		else if (ts<curt)
		{
			System.out.printf("!!!!!!!!!!!!! AjustAggreBolt time sequence disorder\n");
			
		}

		// if (streType.compareTo("qualStre") == 0) {
		//
		//
		// if(ts>curt)
		// {
		// System.out.printf("time stamp %f:   %d\n ",curt,pairRes.size());
		//
		// // for(int i=0;i<pairRes.size();++i)
		// // {
		// //
		// // }
		//
		//
		// curt=ts;
		// pairRes.clear();
		//
		// }
		// else
		// {
		// pairRes.add(pairstr);
		// }
		//
		//
		//
		// // if (ts > curtstampDirec) {
		// //
		// // try {
		// //// fstream = new FileWriter("naiveRes.txt", true);
		// // BufferedWriter out = new BufferedWriter(fstream);
		// //
		// //
		// out.write("Timestamp  "+Double.toString(curtstampDirec)+", "+"total num  "+Integer.toString(pairDirec.size()));
		// //
		// //// for(int i=0;i<qualPairCnt;++i)
		// //// {
		// //// out.write(qualPair[i]+"   ");
		// //// }
		// // out.write("\n");
		// // out.close();
		// // } catch (IOException e) {
		// // // TODO Auto-generated catch block
		// // e.printStackTrace();
		// // }
		// //
		// //
		// System.out.printf("time stamp %f:   %d\n ",curtstampDirec,pairDirec.size());
		// //
		// // paircntDirec = 0;
		// // curtstampDirec = ts;
		// //
		// //
		// // pairDirec.clear();
		// // pairDirec.add(pairstr);
		// //
		// // } else if (ts < curtstampDirec) {
		// // System.out
		// //
		// .printf("!!!!!!!!!!!!! direct AjustAggreBolt time sequence disorder\n");
		// // } else if (Math.abs(ts - curtstampDirec) <= 1e-3) {
		// //
		// // pairDirec.add(pairstr);
		// //
		// // }
		// } else if (streType.compareTo("interQualStre") == 0) {
		//
		//
		// if(ts>curt)
		// {
		// System.out.printf("time stamp %f:   %d\n ",curt,pairRes.size());
		// curt=ts;
		// pairRes.clear();
		// }
		// else
		// {
		// pairRes.add(pairstr);
		// }
		//
		//
		//
		//
		// // if (ts > curtstampInter) {
		// //
		// // try {
		// //// fstream = new FileWriter("naiveRes.txt", true);
		// // BufferedWriter out = new BufferedWriter(fstream);
		// //
		// out.write("Timestamp  "+Double.toString(curtstampInter)+", "+"total num  "+Integer.toString(pairInter.size()));
		// //
		// //// for(int i=0;i<qualPairCnt;++i)
		// //// {
		// //// out.write(qualPair[i]+"   ");
		// //// }
		// // out.write("\n");
		// // out.close();
		// // } catch (IOException e) {
		// // // TODO Auto-generated catch block
		// // e.printStackTrace();
		// // }
		// //
		// //
		// System.out.printf("time stamp %f:   %d\n ",curtstampInter,pairInter.size());
		// //
		// // curtstampInter = ts;
		// // paircntInter = 0;
		// // pairInter.clear();
		// //
		// // pairInter.add(pairstr);
		// //
		// // } else if (ts < curtstampInter) {
		// // System.out
		// //
		// .printf("!!!!!!!!!!!!! inter AjustAggreBolt time sequence disorder\n");
		// // } else if (Math.abs(ts - curtstampInter) <= 1e-3) {
		// //
		// // pairInter.add(pairstr);
		// // }
		// }

		return;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
	}

}
