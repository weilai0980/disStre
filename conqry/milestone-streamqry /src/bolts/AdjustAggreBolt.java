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

	// double curtstamp = TopologyMain.winSize-1;

	double curtstampInter = TopologyMain.winSize - 1;
	double curtstampDirec = TopologyMain.winSize - 1;

	// int rescnt = 0;
	// int[] taskIdx = new int[TopologyMain.nstream];
	// int[] taskids = new int[TopologyMain.nstream];
	// int taskcnt = 0;

	// int[] strid = new int[TopologyMain.nstream];
	// int stridcnt = 0;

	int paircntDirec = 0, paircntInter = 0;
	// int[][] pairIdxDirec = new
	// int[TopologyMain.nstream][TopologyMain.nstream];
	// int[][] pairIdxInter = new
	// int[TopologyMain.nstream][TopologyMain.nstream];

	HashSet<String> pairInter = new HashSet<String>();
	HashSet<String> pairDirec = new HashSet<String>();

	FileWriter fstream; // =new FileWriter("", true);
	BufferedWriter out; // = new BufferedWriter(fstream);

	String streType = new String();

	public static int glAggBolt = 0;
	public int locAggBolt = 0;

	// public int localStreIdx(int hostid) {
	// int i = 0;
	// for (i = 0; i < stridcnt; ++i) {
	// if (strid[i] == hostid) {
	// break;
	// }
	// }
	// if (i == stridcnt) {
	// strid[stridcnt++] = hostid;
	// }
	// return i;
	//
	// }

	// public int localPairStreIdxDirec(int stre1, int stre2) {
	//
	// // int streId[] = new int[4];
	// // tspairStrAna(pairstr, streId);
	//
	// // System.out.printf("%d  %d\n",streId[0],streId[1]);
	//
	// int streNo1 = localStreIdx(stre1);
	// int streNo2 = localStreIdx(stre2);
	// if (pairIdxDirec[streNo1][streNo2] == 0) {
	// pairIdxDirec[streNo1][streNo2] = 1;
	// pairIdxDirec[streNo2][streNo1] = 1;
	// return 1;
	// } else {
	// return 0;
	// }
	// }
	//
	// public int localPairStreIdxInter(int stre1, int stre2) {
	//
	// // int streId[] = new int[4];
	// // tspairStrAna(pairstr, streId);
	//
	// // System.out.printf("%d  %d\n",streId[0],streId[1]);
	//
	// int streNo1 = localStreIdx(stre1);
	// int streNo2 = localStreIdx(stre2);
	// if (pairIdxInter[streNo1][streNo2] == 0) {
	// pairIdxInter[streNo1][streNo2] = 1;
	// pairIdxInter[streNo2][streNo1] = 1;
	// return 1;
	// } else {
	// return 0;
	// }
	// }
	//
	// public void localIdxRenewDirec() {
	//
	// stridcnt = 0;
	// int j = 0;
	// for (int i = 0; i < TopologyMain.nstream; ++i) {
	// for (j = 0; j < TopologyMain.nstream; ++j)
	// pairIdxDirec[i][j] = 0;
	// }
	//
	// return;
	// }

	// public void localIdxRenewInter() {
	//
	// stridcnt = 0;
	// int j = 0;
	// for (int i = 0; i < TopologyMain.nstream; ++i) {
	// for (j = 0; j < TopologyMain.nstream; ++j)
	// pairIdxInter[i][j] = 0;
	// }
	//
	// return;
	// }

	@Override
	public void cleanup() {
		try {
			fstream = new FileWriter("naiveRes.txt", true);
			BufferedWriter out = new BufferedWriter(fstream);
			out.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		try {
			fstream = new FileWriter("naiveRes.txt", false);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		BufferedWriter out = new BufferedWriter(fstream);

		locAggBolt = glAggBolt++;

		return;
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub

		double ts = input.getDoubleByField("ts");
		String pairstr = input.getStringByField("pair");

		streType = input.getSourceStreamId();

		if (streType.compareTo("qualStre") == 0) {
			if (ts > curtstampDirec) {

				paircntDirec = 0;
				curtstampDirec = ts;

				pairDirec.clear();
				if (pairDirec.contains(pairstr) == false) {
					paircntDirec++;
					pairDirec.add(pairstr);
				}

			} else if (ts < curtstampDirec) {
				System.out
						.printf("!!!!!!!!!!!!! direct AjustAggreBolt time sequence disorder\n");
			} else if (Math.abs(ts - curtstampDirec) <= 1e-3) {

				if (pairDirec.contains(pairstr) == false) {
					paircntDirec++;
					pairDirec.add(pairstr);
				}
			}
		} else if (streType.compareTo("interQualStre") == 0) {
			if (ts > curtstampInter) {

				// ..........test...................//

//				if (curtstampInter == 45) {
//					System.out.printf(
//							"AggreBolt %d time stamp %f  has  %s :  \n",
//							locAggBolt, curtstampInter, paircntInter);
//					for (String s : pairInter) {
//						System.out.println(s + ",  ");
//					}
//					System.out.printf("\n");
//				}
				// ................................//

				curtstampInter = ts;
				paircntInter = 0;
				pairInter.clear();

				if (pairInter.contains(pairstr) == false) {
					paircntInter++;
					pairInter.add(pairstr);
				}

			} else if (ts < curtstampInter) {
				System.out
						.printf("!!!!!!!!!!!!! inter AjustAggreBolt time sequence disorder\n");
			} else if (Math.abs(ts - curtstampInter) <= 1e-3) {

				// ..........test........................//
				// if(pairstr.compareTo("9,14")==0 && curtstampInter==42)
				// {
				// System.out.printf("++++++++++++++++++  appear \n");
				// }
				// ......................................//

				if (pairInter.contains(pairstr) == false) {
					paircntInter++;
					pairInter.add(pairstr);
				}
			}

		}

		return;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
	}

}
