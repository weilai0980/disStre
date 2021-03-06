package bolts;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import main.TopologyMain;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class AdjustPreBolt extends BaseBasicBolt {

	public static int gtaskId = 0;
	public int taskId = 0;

	public double curtstamp = 0.0;
	public double ststamp = 0.0;

	public double[][] strevec = new double[TopologyMain.nstreBolt + 10][TopologyMain.winSize + 10];
	public double[][] normvec = new double[TopologyMain.nstreBolt + 10][TopologyMain.winSize + 10];

	public int[] streid = new int[TopologyMain.nstreBolt + 10];
	public int streidCnt = 0;

	public int[] vecst = new int[TopologyMain.nstreBolt + 10];
	public int[] veced = new int[TopologyMain.nstreBolt + 10];
	public int queueLen = TopologyMain.winSize + 5;

	public int[] vecflag = new int[TopologyMain.nstreBolt + 10];

	public int iniFlag = 1;

	// HashMap<String, String> hashMap = new HashMap<String, String>();

	// .........affine relation graph......................//
	
//	List<List<Integer>> graphmat = new ArrayList<List<Integer>>(
//			TopologyMain.nstreBolt + 5);
//	
//	List<List<Integer>> adjList = new ArrayList<List<Integer>>(
//			TopologyMain.nstreBolt + 5);
	
	
//	public int[][]  = new int[TopologyMain.nstreBolt + 10][TopologyMain.nstreBolt + 10];
	
	public int[][] graphmat = new int[TopologyMain.nstreBolt + 10][TopologyMain.nstreBolt + 10];
	public int[] degree = new int[TopologyMain.nstreBolt + 10];

	public int[][] adjList = new int[TopologyMain.nstreBolt + 10][TopologyMain.nstreBolt + 10];
	public int[] adjcnt = new int[TopologyMain.nstreBolt + 10];

	// ...................................................//

	public double disThre = 2 - 2 * TopologyMain.thre;

	public static int adjustBoltNo = 0;
	public int localBoltNo = 0;

	
	
	//................test..................//

	
	public int ta=0,tb=16;
	public double tt=21;
	//......................................//
	
	
	
	public double[] curexp = new double[TopologyMain.nstreBolt + 10],
			curdev = new double[TopologyMain.nstreBolt + 10],
			cursqr = new double[TopologyMain.nstreBolt + 10],
			cursum = new double[TopologyMain.nstreBolt + 10];

	int correCalDis(int vecid1, int vecid2, double thre) {
		double dis = 0.0, tmp = 0.0;
		int k = vecst[vecid1];

		if (Math.abs(curdev[vecid1] - 0.0) < 1e-6
				|| Math.abs(curdev[vecid2] - 0.0) < 1e-6) {

			return 0;
		}

		while (k != veced[vecid1]) {

			normvec[vecid1][k] = (strevec[vecid1][k] - curexp[vecid1])
					/ Math.sqrt(curdev[vecid1]);
			normvec[vecid2][k] = (strevec[vecid2][k] - curexp[vecid2])
					/ Math.sqrt(curdev[vecid2]);

			tmp += ((normvec[vecid1][k] - normvec[vecid2][k]) * (normvec[vecid1][k] - normvec[vecid2][k]));
			k = (k + 1) % queueLen;
		}
		dis = tmp;
		
		
		
		
		
		//.......test.......................//
		
//		if(curtstamp==tt)
//		{
//			
//			if((streid[vecid1]==ta && streid[vecid2]==tb) || (streid[vecid1]==tb && streid[vecid2]==ta)  )
//			{
//				System.out.printf("------- prebolt dis between %d and %d : %f  %f\n \n",streid[vecid1],streid[vecid2],dis, 2 - 2 * thre);	
//			}
//		}
		
		//..................................//
		

		return (dis <= 2 - 2 * thre) ? 1 : 0;
	}

	public void graphCons(double thre, BasicOutputCollector collector,
			double curtime) {

		int i = 0, tmp;
		for (int j = 0; j < streidCnt; ++j) {
			for (i = j + 1; i < streidCnt; ++i) {
				tmp = correCalDis(j, i, thre);
				
				
				//.......test.......................//
				
//				if(curtstamp==tt)
//				{
//					
//					if((streid[j]==ta && streid[i]==tb) || (streid[i]==tb && streid[j]==ta)  )
//					{
//						System.out.printf("------- prebolt dis between %d and %d : %d \n \n",streid[i],streid[j],tmp);	
//					}
//				}
				
				//..................................//
				
				
				

				if (tmp == 1) {
					collector.emit("qualStre",
							new Values(Integer.toString(streid[j]) + ","
									+ Integer.toString(streid[i]), curtime));

					degree[i]++;
					degree[j]++;
				}

				graphmat[j][i] = tmp;
				graphmat[i][j] = tmp;
			}
		}
		return;
	}

	// .....select pivot streams...............................//
	public int bfs(int st, int sel[]) {
		degree[st] = -1;
		int maxdeg = -1, maxstre = -1, cnt = 0;

		for (int i = 0; i < streidCnt; ++i) {
			if (graphmat[st][i] == 1 && degree[i] > 0) {
				degree[i] = -1;
				adjList[st][adjcnt[st]++] = i;

				for (int j = 0; j < streidCnt; ++j) {
					if (graphmat[i][j] == 1 && degree[j] > 0) {
						degree[j]--;
					}
				}
			}
		}
		for (int j = 0; j < streidCnt; ++j) {
			if (degree[j] >= 0) {
				cnt++;
				if (degree[j] > maxdeg) {
					maxstre = j;
					maxdeg = degree[j];
				}
			}
		}
		sel[0] = maxstre;
		return cnt;
	}

	public int affineSelec(int pivots[]) {

		int maxdeg = -1, pstre = 0, pivotnum = 0;
		int[] selstre = new int[3];
		int res = 0;

		for (int j = 0; j < streidCnt; ++j) {
			if (degree[j] > maxdeg) {
				maxdeg = degree[j];
				pstre = j;
			}
		}
		pivots[pivotnum++] = pstre;

		res = bfs(pstre, selstre);
		while (res > 0) {

			pstre = selstre[0];
			pivots[pivotnum++] = pstre;

			res = bfs(pstre, selstre);
		}

		return pivotnum;
	}

	// ..............................................................//

	public String affineCal(int priidx, int secidx) {// ax+b: coef[1]:a
														// coef[0]:b

		int k = 0;
		double primsum = 0.0, secsum = 0.0, prisec = 0.0, unitsum = TopologyMain.winSize;
		double a0 = 0.0, a1 = 0.0, det = 0.0;

		k = vecst[priidx];
		while (k != veced[priidx]) {

			primsum += normvec[priidx][k];
			secsum += normvec[secidx][k];
			prisec += normvec[priidx][k] * normvec[secidx][k];

			k = (k + 1) % queueLen;
		}

		det = 1.0 * unitsum - primsum * primsum;
		a1 = (unitsum * prisec - primsum * secsum) / det;
		
		a0 = (-primsum * prisec + 1.0 * secsum) / det;
//		a0 = (-primsum * prisec - 1.0 * secsum) / det;

		k = vecst[priidx];
//		String errstr = new String(); //residual error vector
//		errstr = "";
		double tmperr = 0.0, normerr = 0.0;

		while (k != veced[priidx]) {

			tmperr = a1 * normvec[priidx][k] + a0 - normvec[secidx][k];

			normerr = normerr + tmperr * tmperr;
//			errstr = errstr + "," + Double.toString(tmperr);

			k = (k + 1) % queueLen;
		}

		return Double.toString(a1) + "," + Double.toString(a0) + ","
				+ Double.toString(normerr);
	}

	public String componGridCoor(int idx) { // different from grid-based
											// approach
		String coorstr = new String();
		coorstr = "";
		int k = 0;
		k = vecst[idx];
		int tmp = 0;
		while (k != veced[idx]) {

			if (normvec[idx][k] >= 0) {
	
				tmp = (int) Math.floor((double) normvec[idx][k]
						/ Math.sqrt(disThre));  // watch out for: it is floor here.

			} else {
				tmp = -1
						* (int) Math.ceil((double) -1 * normvec[idx][k]
								/ Math.sqrt(disThre));
			}

			coorstr = coorstr + Integer.toString(tmp) + ",";

			k = (k + 1) % queueLen;
		}

		return coorstr;
	}

	public void componConsAffine(int idx, String affvec[]) {

		String affstr = new String(), adjidx = new String();
		int k = 0;
		affstr = "";
		adjidx = "";

		k = vecst[idx];
		while (k != veced[idx]) {
			affstr = affstr + Double.toString(normvec[idx][k]) + ",";
			k = (k + 1) % queueLen;
		}
		affvec[0] = affstr;// pivot stream vector
		affstr = "";

		for (int i = 0; i < adjcnt[idx]; ++i) {
			affstr = affstr + affineCal(idx, adjList[idx][i]) + ";";

			adjidx = adjidx + Integer.toString(streid[adjList[idx][i]]) + ",";

		}
		affvec[1] = affstr; // affine relations with pivot stream
		affvec[2] = adjidx;

		return;
	}

	public void idxNewTuple(int strid, double val, int flag) {
		int i = 0, tmpsn = 0;
		double oldval = 0.0, newval = 0.0;

		for (i = 0; i < streidCnt; ++i) {
			if (streid[i] == strid) {
				tmpsn = i;
				break;
			}
		}
		if (i == streidCnt) {
			streid[i] = strid;
			tmpsn = streidCnt;
			streidCnt++;

		}

		if (vecflag[tmpsn] == 0) {

			strevec[tmpsn][veced[tmpsn]] = val;
			veced[tmpsn] = (veced[tmpsn] + 1) % queueLen;

			oldval = strevec[tmpsn][vecst[tmpsn]];
			newval = val;

			vecst[tmpsn] = (vecst[tmpsn] + 1 * flag) % queueLen;

			curexp[tmpsn] = curexp[tmpsn] - oldval / TopologyMain.winSize
					* flag + newval / TopologyMain.winSize;
			cursqr[tmpsn] = cursqr[tmpsn] - oldval * oldval * flag + newval
					* newval;
			cursum[tmpsn] = cursum[tmpsn] - oldval * flag + newval;
			curdev[tmpsn] = cursqr[tmpsn] + TopologyMain.winSize
					* curexp[tmpsn] * curexp[tmpsn] - 2 * cursum[tmpsn]
					* curexp[tmpsn];

			vecflag[tmpsn] = 1;

		}
	}

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
	public void prepare(Map stormConf, TopologyContext context) {
		// TODO Auto-generated method stub

		taskId = gtaskId;
		gtaskId++;

		for (int j = 0; j < TopologyMain.nstreBolt + 10; j++) {
			vecst[j] = 0;
			veced[j] = 0;

			vecflag[j] = 0;
			streid[j] = 0;

			curexp[j] = 0;
			curdev[j] = 0;
			cursqr[j] = 0;
			cursum[j] = 0;

		}

		localBoltNo = adjustBoltNo;
		adjustBoltNo++;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("interStre", new Fields("pidx", "pivotvec",
				"adjaffine", "adjidx", "coord", "ts", "bolt"));

		declarer.declareStream("qualStre", new Fields("pair", "ts"));
		return;
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub

		double ts = input.getDoubleByField("ts");
		double tmpval = input.getDoubleByField("value");
		int sn = input.getIntegerByField("sn");
		int i = 0, tmppivot = 0;

		int[] pivotset = new int[TopologyMain.nstream + 5];
		int pivotcnt = 0;

		String coorStr = new String();
		String[] streAffine = new String[4];

		if (ts > curtstamp) {


			if (ts - ststamp >= TopologyMain.winSize) {

				ststamp++;
				iniFlag = 0;

				graphCons(TopologyMain.thre, collector, curtstamp);
				pivotcnt = affineSelec(pivotset);

				// .........test...............................//
				//
				
				// int [] testsign=new int[TopologyMain.nstream + 5];
				//
				// for(i=0;i<TopologyMain.nstream + 5;++i)
				// testsign[i]=0;
				//
				// // System.out.printf("+++  time %f: \n", curtstamp);
				//
				// for(i=0;i<pivotcnt;i++)
				// {
				// tmppivot=pivotset[i];
				// testsign[tmppivot]++;
				//
				// for(int j=0;j<adjcnt[tmppivot]; ++j)
				//
				// testsign[adjList[tmppivot][j]]++;
				// }
				//
				// for(i=0;i<streidCnt;++i)
				// {
				// if(testsign[i]>=2 || testsign[i]==0)
				// {
				// System.out.printf("+++pivot pair problem at time %f: %d\n",
				// curtstamp,i);
				// }
				// }
				//

				// ............................................//

				// System.out.printf("+++  time %f: \n", curtstamp);

				for (i = 0; i < pivotcnt; ++i) {

					tmppivot = pivotset[i];

					componConsAffine(tmppivot, streAffine);
					coorStr = componGridCoor(tmppivot);

					// .........test.................//

				
					
//					if(curtstamp==tt && streid[tmppivot]==0)
//					{
//						System.out.printf("affine streams of %d  :  %s  %s  %s  %s  \n\n",streid[i],streAffine[0], streAffine[1], streAffine[2],coorStr);
//						
//						
//						
////						
////						for (int k = 0; k < adjcnt[tmppivot]; ++k) {
////
////							System.out.printf(" %d ",streid[adjList[tmppivot][k]]);
////
////						}
//						System.out.printf("\n");
//					}
					
					
					
					// ..............................//

					
//					declarer.declareStream("interStre", new Fields("pidx", "pivotvec",
//							"adjaffine", "adjidx", "coord", "ts", "bolt"));

					
					collector.emit("interStre", new Values(streid[tmppivot],
							streAffine[0], streAffine[1], streAffine[2],
							coorStr, curtstamp, localBoltNo));

				}
			}

			// .....update for next tuple...............//
			curtstamp = ts;
			for (int j = 0; j < TopologyMain.nstreBolt + 5; ++j) {

				degree[j] = 0;
				adjcnt[j] = 0;

				// degree[j]++;

				vecflag[j] = 0;
			}
			idxNewTuple(sn, tmpval, 1 - iniFlag);

		} else if (ts < curtstamp) {
			System.out
					.printf("!!!!!!!!!!!!! AdjustPreBolt time sequence disorder\n");
		} else if (Math.abs(ts - curtstamp) <= 1e-3) {

			idxNewTuple(sn, tmpval, 1 - iniFlag);
		}

	}

	// double correCalDis(int vecidx1, int vecidx2) {
	// double dis = 0.0, tmp = 0.0;
	// int k = vecst[vecidx1];
	// while (k != veced[vecidx1]) {
	//
	// tmp += ((strevec[vecidx1][k] - strevec[vecidx2][k]) *
	// (strevec[vecidx1][k] - strevec[vecidx2][k]));
	// k = (k + 1) % TopologyMain.winSize;
	// }
	// dis = tmp;
	//
	// return (dis - 2.0) / (-2.0);
	// }

}
