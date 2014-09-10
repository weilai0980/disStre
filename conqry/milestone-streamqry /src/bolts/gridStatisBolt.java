package bolts;

import main.*;

import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class gridStatisBolt extends BaseBasicBolt {

	double curtstamp = TopologyMain.winSize-1;

	int[] strid = new int[TopologyMain.nstream+10];
	int stridcnt = 0;
	String[] vecIdx = new String[TopologyMain.nstream+10];

	String[] gridIdx = new String[TopologyMain.gridIdxN];
	int gridIdxcnt = 0;

	int[][] gridStre = new int[TopologyMain.gridIdxN][TopologyMain.gridIdxN];
	int[] gridStreCnt = new int[TopologyMain.gridIdxN];

	String[] qualPair = new String[TopologyMain.gridIdxN+100];

	public int vecAna(String orgstr, double vecval[]) {

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

	public double deviCal(double vec[], double exp, int l) {
		double sum = 0.0;
		for (int i = 0; i < l; ++i) {
			sum += ((vec[i] - exp) * (vec[i] - exp));
		}

		return sum / l;
	}

	public double expCal(double vec[], int l) {
		double sum = 0.0;
		for (int i = 0; i < l; ++i) {
			sum += vec[i];
		}

		return (double) sum / l;
	}

	int correCalDis(String str1, String str2, double thre, int streid1, int streid2, int gridno, double val[]) {
		double dis = 0.0, tmp = 0.0;
		double[] vec1 = new double[TopologyMain.winSize + 10];
		double[] vec2 = new double[TopologyMain.winSize + 10];
		int len = vecAna(str1, vec1);
		vecAna(str2, vec2);

		for (int i = 0; i < len; ++i) {
			tmp += ((vec1[i] - vec2[i]) * (vec1[i] - vec2[i]));
		}
		dis = tmp;
		
		
//		if(streid1==0  && streid2==1)
//		{
//			System.out.printf("timestamp %f:  stream 0 and 1 correlation: %f   in grid: %s \n",curtstamp, Math.sqrt((2-dis)/2), gridIdx[gridno]);
//		}
//		val[0]=dis;
		val[0]=(2.0-dis)/2.0;
		
		return (dis <= 2 - 2 * thre) ? 1 : 0;
	}



	public int correGrid(int gridNo, double thre, String strePair[]) {

		int rescnt = 0, i = 0, j = 0, tmpcnt = gridStreCnt[gridNo];
		int stre1 = 0, stre2 = 0;
		
		String vecstr = new String();
		double [] cval=new double[2];
		
		for (i = 0; i < tmpcnt; ++i) {

			stre1 = gridStre[gridNo][i];
			vecstr = vecIdx[stre1];

			for (j = i + 1; j < tmpcnt; j++) {

				stre2 = gridStre[gridNo][j];

				if (correCalDis(vecstr, vecIdx[stre2], thre,strid[stre1],strid[stre2],gridNo,cval) == 1) {
					if (strid[stre1] > strid[stre2]) {
						
						strePair[rescnt++] = Integer.toString(strid[stre2])
						+ "," + Integer.toString(strid[stre1])+","+Double.toString(cval[0]);
						
//						strePair[rescnt++] = Integer.toString(strid[stre2])
//								+ "," + Integer.toString(strid[stre1]);
					} else {
						
						strePair[rescnt++] = Integer.toString(strid[stre1])
						+ "," + Integer.toString(strid[stre2])+","+Double.toString(cval[0]);
						
						
//						strePair[rescnt++] = Integer.toString(strid[stre1])
//								+ "," + Integer.toString(strid[stre2]);
					}
				}
			}
		}

		return rescnt;
	}

	public int localStreIdx(int hostid, String vecdata) {
		int i = 0;
		for (i = 0; i < stridcnt; ++i) {
			if (strid[i] == hostid) {
				break;
			}
		}
		if (i == stridcnt) {
			strid[stridcnt++] = hostid;
		}
		vecIdx[i] = vecdata;
		return i;
	}

	public int localGridIdx(String grid) {

		int i = 0;
		for (i = 0; i < gridIdxcnt; ++i) {
			if (grid.compareTo(gridIdx[i]) == 0) {
				break;
			}
		}
		if (i == gridIdxcnt) {
			gridIdx[gridIdxcnt++] = grid;
		}
		return i;
	}

	public void localGridStreIdx(String grid, int hostid, String vecdata) {
		int streNo = localStreIdx(hostid, vecdata);
		int gridNo = localGridIdx(grid);

		gridStre[gridNo][gridStreCnt[gridNo]++] = streNo;

		return;
	}
	public void localIdxRenew() {

		stridcnt = 0;
		for (int i = 0; i < gridIdxcnt + 1; ++i) {
			gridStreCnt[i] = 0;
		}
		gridIdxcnt = 0;

		return;
	}

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
		// TODO Auto-generated method stub
		declarer.declare(new Fields("ts", "pair"));
	
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		
		
		return;
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub

		double ts = input.getDoubleByField("ts");
		String coordstr = input.getStringByField("coord");
		String vecstr = input.getStringByField("vec");
		int hostid = input.getIntegerByField("hostid");

		int resnum = 0;

		int tmpcnt=0;
		if (ts > curtstamp) {

		
			tmpcnt=0;

			for (int i = 0; i < gridIdxcnt; ++i) {
				
								
				resnum = correGrid(i, TopologyMain.thre, qualPair);
				
				for (int j = 0; j < resnum; ++j) {
					
					tmpcnt++;
					collector.emit(new Values(curtstamp, qualPair[j]));
				}
			}
			

			
			localIdxRenew();
			localGridStreIdx(coordstr, hostid, vecstr);
			curtstamp = ts;
			
		} else if (ts < curtstamp) {
			System.out.printf("!!!!!!!!!!!!! time sequence disorder\n");
		} else if (Math.abs(ts - curtstamp) <= 1e-3) {

			localGridStreIdx(coordstr, hostid, vecstr);

		}
		return;
	}
}


//public int correCal(String str1, String str2, double thre) {
//double cor = 0.0, tmp = 0.0;
//double[] vec1 = new double[TopologyMain.winSize + 10];
//double[] vec2 = new double[TopologyMain.winSize + 10];
//int len = vecAna(str1, vec1);
//vecAna(str2, vec2);
//
//double exp1 = expCal(vec1, len);
//double exp2 = expCal(vec2, len);
//
//double dev1 = deviCal(vec1, exp1, len);
//double dev2 = deviCal(vec2, exp2, len);
//
//for (int i = 0; i < len; ++i) {
//	tmp += ((vec1[i] - exp1) * (vec2[i] - exp2));
//}
//cor = tmp / (Math.sqrt(dev1) * Math.sqrt(dev2) * len);
//
//return (cor >= thre) ? 1 : 0;
//}
