package bolts;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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

public class AdjustApproBolt extends BaseBasicBolt {

	double curtstamp = TopologyMain.winSize - 1;

	int[][] gridCoors = new int[TopologyMain.gridIdxN + 5][TopologyMain.winSize + 5];
	double[][] pivotVec = new double[TopologyMain.gridIdxN + 5][TopologyMain.winSize + 5];

	int[] gridPivot = new int[TopologyMain.gridIdxN + 5];
	int[] gridBolt = new int[TopologyMain.gridIdxN + 5];
	int[] gpTaskId = new int[TopologyMain.gridIdxN + 5];

	int gridIdxcnt = 0;

	List<List<Double>> gridAffs = new ArrayList<List<Double>>(
			TopologyMain.nstreBolt + 5);

	List<List<Integer>> gridAdjIdx = new ArrayList<List<Integer>>(
			TopologyMain.nstreBolt + 5);

	HashMap<Integer, Integer> pStreMap = new HashMap<Integer, Integer>();

	int sentinel = -100000;
	int boltNo = 0;

	public static int glAppBolt = 0;
	public int locAppBolt = 0;

	// for partition vector

	public double subDivNum = 2;
	public double taskRange = 2.0 / subDivNum;

	public double disThre = 2 - 2 * TopologyMain.thre;

	public int gridRange = (int) Math.ceil(1.0 / Math.sqrt(disThre));
	// public int taskGridMax=2*gridRange-1;

	public double taskGridCap = taskRange / Math.sqrt(disThre);
	public int locTaskId;

	// ...........test...................//
	// public int ta = 3, tb = 8;
	// public double tt = 21;

	// ...................................//

	public int groupTaskId(int cell[][], int id, int dimN) {

		int cellCoor = 0, partCoor = 0;
		int tmpid = 0;

		cellCoor = cell[id][0] + gridRange + 1;
		tmpid = (int) Math.ceil((double) cellCoor / taskGridCap);

		for (int i = 1; i < dimN; ++i) {

			cellCoor = cell[id][i] + gridRange + 1; // coordinate
			// transposition to
			// positive range
			partCoor = (int) Math.ceil((double) cellCoor / taskGridCap);

			tmpid = (int) (tmpid + (partCoor - 1) * Math.pow(subDivNum, i));
		}

		return tmpid - 1;
	}

	public int pivotVecAna(String orgstr, double vecval[][], int id) {

		int len = orgstr.length();
		int cnt = 0, pre = 0;
		for (int i = 0; i < len; ++i) {
			if (orgstr.charAt(i) == ',') {
				vecval[id][cnt++] = Double.valueOf(orgstr.substring(pre, i));
				pre = i + 1;
			}
		}
		return cnt;
	}

	public int pivotCoor(String orgstr, int coor[][], int id) {

		int len = orgstr.length();
		int cnt = 0, pre = 0;
		for (int i = 0; i < len; ++i) {
			if (orgstr.charAt(i) == ',') {
				coor[id][cnt++] = Integer.valueOf(orgstr.substring(pre, i));
				pre = i + 1;
			}
		}

		return cnt;
	}

	public int pivotAffs(String orgstr, double affines[][], int id) {

		int len = orgstr.length();
		int cnt = 0, pre = 0;
		for (int i = 0; i < len; ++i) {
			if (orgstr.charAt(i) == ',' || orgstr.charAt(i) == ';') {
				affines[id][cnt++] = Double.valueOf(orgstr.substring(pre, i));
				pre = i + 1;
			}
		}
		affines[id][cnt++] = sentinel;
		return cnt;
	}

	public int pivotAffs(String orgstr, int id) {

		int len = orgstr.length();
		int cnt = 0, pre = 0;
		for (int i = 0; i < len; ++i) {
			if (orgstr.charAt(i) == ',' || orgstr.charAt(i) == ';') {
				gridAffs.get(id).add(Double.valueOf(orgstr.substring(pre, i)));
				pre = i + 1;
			}
		}
		// gridAffs.get[id][cnt++] = sentinel;
		return cnt;
	}

	public int adjIdx(String orgstr, int adjStre[][], int id) {

		int len = orgstr.length();
		int cnt = 0, pre = 0;
		for (int i = 0; i < len; ++i) {
			if (orgstr.charAt(i) == ',') {
				adjStre[id][cnt++] = Integer.valueOf(orgstr.substring(pre, i));
				pre = i + 1;
			}
		}
		adjStre[id][cnt++] = sentinel;
		return cnt;
	}

	public int adjIdx(String orgstr, int id) {

		int len = orgstr.length();
		int cnt = 0, pre = 0;
		for (int i = 0; i < len; ++i) {
			if (orgstr.charAt(i) == ',') {
				gridAdjIdx.get(id).add(
						Integer.valueOf(orgstr.substring(pre, i)));
				// [id][cnt++] = Integer.valueOf(orgstr.substring(pre, i));
				pre = i + 1;
			}
		}
		// gridAdjIdx[id][cnt++] = sentinel;
		return cnt;
	}

	// public double deviCal(double vec[], double exp, int l) {
	// double sum = 0.0;
	// for (int i = 0; i < l; ++i) {
	// sum += ((vec[i] - exp) * (vec[i] - exp));
	// }
	//
	// return sum / l;
	// }
	//
	// public double expCal(double vec[], int l) {
	// double sum = 0.0;
	// for (int i = 0; i < l; ++i) {
	// sum += vec[i];
	// }
	//
	// return (double) sum / l;
	// }

	// int correCalDis(String str1, String str2, double thre, int streid1,
	// int streid2, int gridno, double val[]) {
	// double dis = 0.0, tmp = 0.0;
	// double[] vec1 = new double[TopologyMain.winSize + 10];
	// double[] vec2 = new double[TopologyMain.winSize + 10];
	// int len = vecAna(str1, vec1);
	// vecAna(str2, vec2);
	//
	// for (int i = 0; i < len; ++i) {
	// tmp += ((vec1[i] - vec2[i]) * (vec1[i] - vec2[i]));
	// }
	// dis = tmp;
	//
	// // if(streid1==0 && streid2==1)
	// // {
	// //
	// System.out.printf("timestamp %f:  stream 0 and 1 correlation: %f   in grid: %s \n",curtstamp,
	// // Math.sqrt((2-dis)/2), gridIdx[gridno]);
	// // }
	// // val[0]=dis;
	// val[0] = (2.0 - dis) / 2.0;
	//
	// return (dis <= 2 - 2 * thre) ? 1 : 0;
	// }

	// public int correGrid(int gridNo, double thre, String strePair[]) {
	//
	// int rescnt = 0, i = 0, j = 0, tmpcnt = gridStreCnt[gridNo];
	// int stre1 = 0, stre2 = 0;
	//
	// String vecstr = new String();
	// double [] cval=new double[2];
	//
	// for (i = 0; i < tmpcnt; ++i) {
	//
	// stre1 = gridStre[gridNo][i];
	// vecstr = vecIdx[stre1];
	//
	// for (j = i + 1; j < tmpcnt; j++) {
	//
	// stre2 = gridStre[gridNo][j];
	//
	// if (correCalDis(vecstr, vecIdx[stre2],
	// thre,strid[stre1],strid[stre2],gridNo,cval) == 1) {
	// if (strid[stre1] > strid[stre2]) {
	//
	// strePair[rescnt++] = Integer.toString(strid[stre2])
	// + "," + Integer.toString(strid[stre1])+","+Double.toString(cval[0]);
	//
	// // strePair[rescnt++] = Integer.toString(strid[stre2])
	// // + "," + Integer.toString(strid[stre1]);
	// } else {
	//
	// strePair[rescnt++] = Integer.toString(strid[stre1])
	// + "," + Integer.toString(strid[stre2])+","+Double.toString(cval[0]);
	//
	//
	// // strePair[rescnt++] = Integer.toString(strid[stre1])
	// // + "," + Integer.toString(strid[stre2]);
	// }
	// }
	// }
	// }
	//
	// return rescnt;
	// }

	// public int localStreIdx(int hostid, String vecdata) {
	// int i = 0;
	// for (i = 0; i < stridcnt; ++i) {
	// if (strid[i] == hostid) {
	// break;
	// }
	// }
	// if (i == stridcnt) {
	// strid[stridcnt++] = hostid;
	// }
	// vecIdx[i] = vecdata;
	// return i;
	// }

	// public int localGridIdx(String grid) {
	//
	// int i = 0;
	// for (i = 0; i < gridIdxcnt; ++i) {
	// if (grid.compareTo(gridIdx[i]) == 0) {
	// break;
	// }
	// }
	// if (i == gridIdxcnt) {
	// gridIdx[gridIdxcnt++] = grid;
	// }
	// return i;
	// }
	//

	public boolean localpStreIdx(int stre) {

		if (pStreMap.containsKey(stre) == true) {
			return false;
		} else {
			pStreMap.put(stre, gridIdxcnt);
			return true;
		}

	}

	public void localIdxRenew() {

		gridAffs.clear();
		gridAdjIdx.clear();

		gridIdxcnt = 0;
		pStreMap.clear();

		return;
	}

	public int checkGrids(int idx1, int idx2) {

		if (gridBolt[idx1] == gridBolt[idx2]) {
			return 0;
		}

		if(gpTaskId[idx1] != locTaskId && gpTaskId[idx2] != locTaskId)
			return 0;
		
		for (int j = 0; j < TopologyMain.winSize; ++j) {

			if ((gridCoors[idx1][j] + 1) < gridCoors[idx2][j] - 1
					|| (gridCoors[idx1][j] - 1) > gridCoors[idx2][j] + 1
					) {

				return 0;
			}
		}

		return 1;
	}

	public int corBtwAffInGrids(int idx1, int idx2, double thre,
			BasicOutputCollector collector, double tStamp) {

		int i = 0, j = 0, cnt = 0, k = 0;
		double tmpdis = 0.0;
		double w11 = 0.0, w10 = 0.0, er1 = 0.0, w21 = 0.0, w20 = 0.0, er2 = 0.0;
		double[] tmpvec = new double[TopologyMain.winSize + 5];
		double tmpscal = 0.0, tmpscal2 = 0.0, tmpdis2 = 0.0;
		int iniflag = 1;
		int adjidx1 = 0, adjidx2 = 0;

		double sqrthre = Math.sqrt(thre);

		Iterator<Double> it1 = gridAffs.get(idx1).iterator();

		// for (Integer i : gridAffs.get(idx1)) {

		// while (gridAffs[idx1][i] != sentinel) {

		while (it1.hasNext()) {

			adjidx2 = 0;
			j = 0;

			w11 = it1.next();
			w10 = it1.next();
			er1 = it1.next();

			// w11 = gridAffs[idx1][i];
			// w10 = gridAffs[idx1][i + 1];
			// er1 = gridAffs[idx1][i + 2];

			tmpdis = 0.0;
			for (k = 0; k < TopologyMain.winSize; ++k) {
				tmpvec[k] = w11 * pivotVec[idx1][k] + w10;

				tmpscal = tmpvec[k] - pivotVec[idx2][k];
				// tmpscal = w11 * pivotVec[idx1][k] + w10 - pivotVec[idx2][k];

				tmpdis += (tmpscal * tmpscal);

			}
			tmpdis = Math.abs(Math.sqrt(tmpdis) - Math.sqrt(er1));

			// ..............test..........................//

			// if (curtstamp == tt) {
			// if (gridPivot[idx2] == 17 && gridAdjIdx[idx1][adjidx1] == 18) {
			// System.out
			// .printf("-------------  correlation %f in bolt %d     %f\n",
			// tmpdis, locAppBolt, sqrthre);
			// }
			// }

			// ...........................................//

			if (tmpdis <= sqrthre) {
				cnt++;

				// .........test.....................//
				// String tmp = Integer.toString(gridAdjIdx[idx1][adjidx1]) +
				// ","
				// + Integer.toString(gridPivot[idx2]);
				//
				// if (tStamp == 45
				// && (tmp.compareTo("17,18") == 0 || tmp
				// .compareTo("18,17") == 0)) {
				// System.out.printf("-------------  %s sent in bolt %d %f\n",
				// tmp, locAppBolt, tStamp);
				// }
				// ..................................//

				if (gridAdjIdx.get(idx1).get(adjidx1) < gridPivot[idx2]) {

					// if (gridAdjIdx[idx1][adjidx1] < gridPivot[idx2]) {

					// collector.emit(
					// "interQualStre",
					// new Values(tStamp, Integer
					// .toString(gridAdjIdx[idx1][adjidx1])
					// + ","
					// + Integer.toString(gridPivot[idx2])));

					collector.emit(
							"interQualStre",
							new Values(tStamp, Integer.toString(gridAdjIdx.get(
									idx1).get(adjidx1))
									+ "," + Integer.toString(gridPivot[idx2])));
				} else {
					// collector
					// .emit("interQualStre",
					// new Values(
					// tStamp,
					// Integer.toString(gridPivot[idx2])
					// + ","
					// + Integer
					// .toString(gridAdjIdx[idx1][adjidx1])));

					collector.emit(
							"interQualStre",
							new Values(tStamp, Integer
									.toString(gridPivot[idx2])
									+ ","
									+ Integer.toString(gridAdjIdx.get(idx1)
											.get(adjidx1))));

				}

			}

			// ................test..........................//

			// if (curtstamp == tt) {
			// if ((gridAdjIdx[idx1][adjidx1] == tb)
			// || (gridAdjIdx[idx2][adjidx2] == ta)) {
			// System.out.printf("!!!!!!!! appear in bolt %d\n\n",
			// locAppBolt);
			// }
			// }

			// ............................................//

			Iterator<Double> it2 = gridAffs.get(idx2).iterator();

			while (it2.hasNext()) {

				// while (gridAffs[idx2][j] != sentinel) {

				// ....................test..................................//
				// if (curtstamp == tt) {
				// if ((gridAdjIdx[idx1][adjidx1] == ta &&
				// gridAdjIdx[idx2][adjidx2] == tb)
				// || (gridAdjIdx[idx1][adjidx1] == tb &&
				// gridAdjIdx[idx2][adjidx2] == ta)) {
				// System.out.printf("!!!!!!!! appear in bolt %d\n\n",
				// locAppBolt);
				// }
				// }

				// if (curtstamp == tt) {
				// if ((gridAdjIdx[idx1][adjidx1] == ta)
				// || (gridAdjIdx[idx2][adjidx2] == ta)) {
				// System.out.printf("!!!!!!!! appear in bolt %d\n\n",
				// locAppBolt);
				// }
				// }

				// ...........................................................//

				w21 = it2.next();
				w20 = it2.next();
				er2 = it2.next();

				// w21 = gridAffs[idx2][j];
				// w20 = gridAffs[idx2][j + 1];
				// er2 = gridAffs[idx2][j + 2];

				tmpdis = 0.0;
				tmpdis2 = 0.0;
				for (k = 0; k < TopologyMain.winSize; ++k) {
					tmpscal = tmpvec[k] - w21 * pivotVec[idx2][k] - w20;
					tmpdis += (tmpscal * tmpscal);

					if (iniflag == 1) {
						tmpscal2 = w21 * pivotVec[idx2][k] + w20
								- pivotVec[idx1][k];
						tmpdis2 += (tmpscal2 * tmpscal2);
					}

				}

//				tmpdis = Math.abs(Math.sqrt(tmpdis) - Math.sqrt(er1)
//						- Math.sqrt(er2));
				
				tmpdis = Math.sqrt(tmpdis) - Math.sqrt(er1)
						- Math.sqrt(er2);
				
				tmpdis = Math.max(tmpdis, -Math.sqrt(tmpdis)  +  Math.abs(Math.sqrt(er1)
						- Math.sqrt(er2)) );
				
				// Math.sqrt(tmpdis) - Math.sqrt(er1)

				if (tmpdis <= sqrthre) {
					cnt++;

					if (gridAdjIdx.get(idx1).get(adjidx1) < gridAdjIdx
							.get(idx2).get(adjidx2))
					// [idx1][adjidx1] < gridAdjIdx[idx2][adjidx2])
					{
						// collector
						// .emit("interQualStre",
						// new Values(
						// tStamp,
						// Integer.toString(gridAdjIdx[idx1][adjidx1])
						// + ","
						// + Integer
						// .toString(gridAdjIdx[idx2][adjidx2])));

						collector.emit(
								"interQualStre",
								new Values(tStamp, Integer.toString(gridAdjIdx
										.get(idx1).get(adjidx1))
										+ ","
										+ Integer.toString(gridAdjIdx.get(idx2)
												.get(adjidx2))));
					} else {
						// collector
						// .emit("interQualStre",
						// new Values(
						// tStamp,
						// Integer.toString(gridAdjIdx[idx2][adjidx2])
						// + ","
						// + Integer
						// .toString(gridAdjIdx[idx1][adjidx1])));

						collector.emit(
								"interQualStre",
								new Values(tStamp, Integer.toString(gridAdjIdx
										.get(idx2).get(adjidx2))
										+ ","
										+ Integer.toString(gridAdjIdx.get(idx1)
												.get(adjidx1))));

					}

				}

				if (iniflag == 1) {
					tmpdis2 = Math.abs(Math.sqrt(tmpdis2) - Math.sqrt(er2));

					// ...................test.......................................//
					// if (curtstamp == tt) {
					// if (gridPivot[idx1] == 17
					// && gridAdjIdx[idx2][adjidx2] == 18) {
					// System.out
					// .printf("-------------  correlation %f in bolt %d      %f\n",
					// tmpdis2, locAppBolt, sqrthre);
					// }
					// }
					// ...............................................................//

					// .........test.....................//
					// String tmp = Integer.toString(gridPivot[idx1]) + ","
					// + Integer.toString(gridAdjIdx[idx2][adjidx2]);
					//
					// if (tStamp == 45
					// && (tmp.compareTo("17,18") == 0 || tmp
					// .compareTo("18,17") == 0)) {
					// System.out.printf(
					// "-------------  %s sent in bolt %d at %f\n",
					// tmp, locAppBolt, tStamp);
					// }
					// ..................................//

					if (tmpdis2 <= sqrthre) {

						cnt++;

						if (gridPivot[idx1] < gridAdjIdx.get(idx2).get(adjidx2)) {
							collector.emit(
									"interQualStre",
									new Values(tStamp, Integer
											.toString(gridPivot[idx1])
											+ ","
											+ Integer.toString(gridAdjIdx.get(
													idx2).get(adjidx2))));
						} else {
							collector
									.emit("interQualStre",
											new Values(
													tStamp,
													Integer.toString(gridAdjIdx
															.get(idx2).get(
																	adjidx2))
															+ ","
															+ Integer
																	.toString(gridPivot[idx1])));

						}

						// if (gridPivot[idx1] < gridAdjIdx[idx2][adjidx2]) {
						// collector
						// .emit("interQualStre",
						// new Values(
						// tStamp,
						// Integer.toString(gridPivot[idx1])
						// + ","
						// + Integer
						// .toString(gridAdjIdx[idx2][adjidx2])));
						// } else {
						// collector
						// .emit("interQualStre",
						// new Values(
						// tStamp,
						// Integer.toString(gridAdjIdx[idx2][adjidx2])
						// + ","
						// + Integer
						// .toString(gridPivot[idx1])));
						//
						// }

					}
				}

				j = j + 3;
				adjidx2++;
			}

			adjidx1++;
			i = i + 3;

			iniflag = 0;
		}

		if (adjidx1 == 0) {
			//
			j = 0;
			//

			Iterator<Double> it2 = gridAffs.get(idx2).iterator();

			while (it2.hasNext()) {

				// while (gridAffs[idx2][j] != sentinel) {
				//

				w21 = it2.next();
				w20 = it2.next();
				er2 = it2.next();

				// w21 = gridAffs[idx2][j];
				// w20 = gridAffs[idx2][j + 1];
				// er2 = gridAffs[idx2][j + 2];
				//
				// // tmpdis = 0.0;
				tmpdis2 = 0.0;
				for (k = 0; k < TopologyMain.winSize; ++k) {
					// tmpscal = tmpvec[k] - w21 * pivotVec[idx2][k] - w20;
					// tmpdis += (tmpscal * tmpscal);
					//
					// if (iniflag == 1) {
					tmpscal2 = w21 * pivotVec[idx2][k] + w20
							- pivotVec[idx1][k];
					tmpdis2 += (tmpscal2 * tmpscal2);
					// }

				}

				// tmpdis = Math.abs(Math.sqrt(tmpdis) - Math.sqrt(er1)
				// - Math.sqrt(er2));
				// Math.sqrt(tmpdis) - Math.sqrt(er1)

				// if (tmpdis <= sqrthre) {
				// cnt++;
				//
				// if (gridAdjIdx[idx1][adjidx1] < gridAdjIdx[idx2][adjidx2]) {
				// collector
				// .emit("interQualStre",
				// new Values(
				// tStamp,
				// Integer.toString(gridAdjIdx[idx1][adjidx1])
				// + ","
				// + Integer
				// .toString(gridAdjIdx[idx2][adjidx2])));
				// } else {
				// collector
				// .emit("interQualStre",
				// new Values(
				// tStamp,
				// Integer.toString(gridAdjIdx[idx2][adjidx2])
				// + ","
				// + Integer
				// .toString(gridAdjIdx[idx1][adjidx1])));
				//
				// }
				//
				// }
				//
				// if (iniflag == 1) {
				tmpdis2 = Math.abs(Math.sqrt(tmpdis2) - Math.sqrt(er2));

				// ...................test.......................................//
				// if (curtstamp == tt) {
				// if (gridPivot[idx1] == 17
				// && gridAdjIdx[idx2][adjidx2] == 18) {
				// System.out
				// .printf("-------------  correlation %f in bolt %d      %f\n",
				// tmpdis2, locAppBolt, sqrthre);
				// }
				// }
				// ...............................................................//

				// .........test.....................//
				// String tmp = Integer.toString(gridPivot[idx1]) + ","
				// + Integer.toString(gridAdjIdx[idx2][adjidx2]);
				//
				// if (tStamp == 45
				// && (tmp.compareTo("17,18") == 0 || tmp
				// .compareTo("18,17") == 0)) {
				// System.out.printf(
				// "-------------  %s sent in bolt %d at %f\n", tmp,
				// locAppBolt,tStamp);
				// }
				// ..................................//

				if (tmpdis2 <= sqrthre) {
					cnt++;

					if (gridPivot[idx1] < gridAdjIdx.get(idx2).get(adjidx2)) {
						collector.emit(
								"interQualStre",
								new Values(tStamp, Integer
										.toString(gridPivot[idx1])
										+ ","
										+ Integer.toString(gridAdjIdx.get(idx2)
												.get(adjidx2))));
					} else {
						collector.emit(
								"interQualStre",
								new Values(tStamp, Integer.toString(gridAdjIdx
										.get(idx2).get(adjidx2))
										+ ","
										+ Integer.toString(gridPivot[idx1])));

					}

					// if (gridPivot[idx1] < gridAdjIdx[idx2][adjidx2]) {
					// collector
					// .emit("interQualStre",
					// new Values(
					// tStamp,
					// Integer.toString(gridPivot[idx1])
					// + ","
					// + Integer
					// .toString(gridAdjIdx[idx2][adjidx2])));
					// } else {
					// collector.emit(
					// "interQualStre",
					// new Values(tStamp, Integer
					// .toString(gridAdjIdx[idx2][adjidx2])
					// + ","
					// + Integer.toString(gridPivot[idx1])));
					//
					// }

				}
				j = j + 3;
				adjidx2++;
			}

		}

		return cnt;
	}

	public int corBtwPivots(int idx1, int idx2, double sqthre,
			BasicOutputCollector collector, double tStamp) {

		int k = 0;
		double tmpscal = 0.0, tmpdis = 0.0;
		for (k = 0; k < TopologyMain.winSize; ++k) {
			tmpscal = pivotVec[idx1][k] - pivotVec[idx2][k];
			tmpdis += (tmpscal * tmpscal);

		}

		// ......................test...............................//
		// if(curtstamp==tt)
		// {
		// if( (gridPivot[idx1]==ta && gridPivot[idx2]==tb) ||
		// (gridPivot[idx2]==ta && gridPivot[idx1]==tb) )
		// {
		// System.out.printf("------------------  pivot stream %d and %d : %f \n\n",
		// gridPivot[idx1],gridPivot[idx2],tmpdis);
		// }
		// }
		// ..........................................................//

		if (tmpdis <= sqthre) {
			if (gridPivot[idx1] < gridPivot[idx2]) {
				collector.emit("interQualStre",
						new Values(tStamp, Integer.toString(gridPivot[idx1])
								+ "," + Integer.toString(gridPivot[idx2])));
			} else {
				collector.emit("interQualStre",
						new Values(tStamp, Integer.toString(gridPivot[idx2])
								+ "," + Integer.toString(gridPivot[idx1])));

			}
			return 1;
		} else {
			return 0;
		}
	}

	// public int corBtwAffPivot(int aff, int pivot, double sqthre) {
	//
	// int i = 0, cnt = 0, k = 0;
	// double tmpdis = 0.0;
	// double w11 = 0.0, w10 = 0.0, er1 = 0.0;// , w21=0.0,w20=0.0,er2=0.0;
	// double tmpscal = 0.0;
	//
	// while (gridAffs[aff][i] != -10000) {
	// // j = 0;
	//
	// w11 = gridAffs[aff][i];
	// w10 = gridAffs[aff][i + 1];
	// er1 = gridAffs[aff][i + 2];
	//
	// for (k = 0; k < TopologyMain.winSize; ++k) {
	//
	// tmpscal = w11 * pivotVec[aff][k] + w10 - pivotVec[pivot][k];
	// tmpdis += (tmpscal * tmpscal);
	//
	// }
	//
	// tmpdis = Math.abs(tmpdis - er1);
	//
	// if (tmpdis < sqthre) {
	// cnt++;
	// }
	// i = i + 3;
	// }
	//
	// return cnt;
	// }

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

		declarer.declareStream("interQualStre", new Fields("ts", "pair"));

	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {

		locAppBolt = glAppBolt++;

		locTaskId = context.getThisTaskIndex();

		return;
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub

		double ts = input.getDoubleByField("ts");
		String coordstr = input.getStringByField("coord");
		String pivotstr = input.getStringByField("pivotvec");
		int pivotId = input.getIntegerByField("pidx");
		String affRelStr = input.getStringByField("adjaffine");
		String affIdx = input.getStringByField("adjidx");
		boltNo = input.getIntegerByField("bolt");

		int tmpid = 0;
		int resnum = 0, i, j;

		if (ts > curtstamp) {

			for (i = 0; i < gridIdxcnt; ++i) {
				for (j = i + 1; j < gridIdxcnt; ++j) {
					if (checkGrids(i, j) == 1) {
						resnum += corBtwAffInGrids(i, j,
								2 - 2 * TopologyMain.thre, collector, curtstamp);
						resnum += corBtwPivots(i, j, 2 - 2 * TopologyMain.thre,
								collector, curtstamp);

					}
					// ..................test......................................//
					// if (curtstamp == tt) {
					// if ((gridPivot[i] == 17 && gridPivot[j] == 16)
					// || (gridPivot[i] == 16 && gridPivot[j] == 17)) {
					// System.out.printf(
					// " **************  Appear %d in bolt %d \n",
					// checkGrids(i, j), locAppBolt);
					//
					// // System.out.printf(
					// // "------------------  pivot stream %d : ",
					// // pivotId);
					// // for (int k = 0; k < TopologyMain.winSize; ++k) {
					// // System.out.printf(" %d  ", gridCoors[i][k]);
					// // }
					// // System.out.printf("\n");
					// // for (int k = 0; k < TopologyMain.winSize; ++k) {
					// // System.out.printf(" %d  ", gridCoors[j][k]);
					// // }
					// // System.out.printf("\n");
					//
					// }
					// }
					// ...........................................................//
				}

				// ....................test..................................//
				// if (curtstamp == tt) {
				// if( (gridPivot[i] == ta && gridPivot[j] == tb) ||
				// (gridPivot[i] == tb && gridPivot[j] == ta) ) {
				// System.out.printf("!!!!!!!! appear in bolt %d\n\n",
				// locAppBolt);
				// }
				// }
				// ...........................................................//

			}

			// ..........test....................//
			// System.out.printf(
			// "Number of qualifeid stream pairs at time %f:  %d\n",
			// curtstamp, resnum);
			// ..................................//

			// for (int i = 0; i < resnum; ++i) {
			// collector.emit("interQualStre", new Values(curtstamp, 1, 2));
			// }
			// ...........................................................//
			localIdxRenew();
			if (localpStreIdx(pivotId) == true) {
				tmpid = gridIdxcnt++;

				gridAffs.add(new ArrayList<Double>());
				gridAdjIdx.add(new ArrayList<Integer>());

				pivotVecAna(pivotstr, pivotVec, tmpid);
				pivotCoor(coordstr, gridCoors, tmpid);

				pivotAffs(affRelStr, tmpid);
				// adjIdx(affIdx, gridAdjIdx, tmpid);
				adjIdx(affIdx, tmpid);
				gridBolt[tmpid] = boltNo;
				gridPivot[tmpid] = pivotId;
				gpTaskId[tmpid] = groupTaskId(gridCoors, tmpid,
						TopologyMain.winh);
			}

			// ...................test...................................//

			// if (ts == tt) {
			//
			// if ((pivotId == ta || pivotId == tb)) {
			// System.out
			// .printf("ApproBolt %d ------------------  pivot stream %d :  \n\n",
			// locAppBolt, pivotId);
			//
			// // for (int k = 0; k < TopologyMain.winSize; ++k) {
			// // System.out.printf(" %d  ", gridCoors[tmpid][k]);
			// // }
			// // System.out.printf("\n");
			//
			// }
			// }
			//
			// j = 0;
			// while (gridAffs[tmpid][j] != sentinel) {
			// if (gridAdjIdx[tmpid][j] == ta
			// || gridAdjIdx[tmpid][j] == tb) {
			// System.out
			// .printf("ApproBolt %d ------------------ stream %d %d \n",
			// locAppBolt, gridAdjIdx[tmpid][j],
			// pivotId);
			//
			// // for (int k = 0; k < TopologyMain.winSize; ++k) {
			// // System.out.printf(" %d  ", gridCoors[tmpid][k]);
			// // }
			// // System.out.printf("\n");
			// }
			// j++;
			// }
			// }

			// ...........................................................//

			curtstamp = ts;

		} else if (ts < curtstamp) {
			System.out
					.printf("!!!!!!!!!!!!! AdjustApproBolt time sequence disorder\n");
		} else if (Math.abs(ts - curtstamp) <= 1e-3) {

			// ..................test......................................//
			// if (curtstamp == tt) {
			// if ((pivotId == ta || pivotId == tb)) {
			// System.out
			// .printf("ApproBolt %d ------------------  pivot stream %d :  \n\n",
			// locAppBolt, pivotId);
			// }
			//
			// }
			// ...........................................................//

			if (localpStreIdx(pivotId) == true) {
				tmpid = gridIdxcnt++;

				gridAffs.add(new ArrayList<Double>());
				gridAdjIdx.add(new ArrayList<Integer>());

				pivotVecAna(pivotstr, pivotVec, tmpid);
				pivotCoor(coordstr, gridCoors, tmpid);
				// pivotAffs(affRelStr, gridAffs, tmpid);
				pivotAffs(affRelStr, tmpid);
				// adjIdx(affIdx, gridAdjIdx, tmpid);
				adjIdx(affIdx, tmpid);
				gridBolt[tmpid] = boltNo;
				gridPivot[tmpid] = pivotId;
				gpTaskId[tmpid] = groupTaskId(gridCoors, tmpid,
						TopologyMain.winh);

				// ...................test...................................//
				//
				// if (curtstamp == tt) {
				//
				// if ((pivotId == ta || pivotId == tb)) {
				// System.out
				// .printf("ApproBolt %d ------------------  pivot stream %d :  \n\n",
				// locAppBolt, pivotId);
				//
				// // for (int k = 0; k < TopologyMain.winSize; ++k) {
				// // System.out.printf(" %d  ", gridCoors[tmpid][k]);
				// // }
				// // System.out.printf("\n");
				//
				// }
				// }
				//
				// j = 0;
				// while (gridAffs[tmpid][j] != sentinel) {
				// if (gridAdjIdx[tmpid][j] == ta
				// || gridAdjIdx[tmpid][j] == tb) {
				// System.out
				// .printf("ApproBolt %d ------------------ stream %d %d \n",
				// locAppBolt, gridAdjIdx[tmpid][j],
				// pivotId);
				//
				// // for (int k = 0; k < TopologyMain.winSize; ++k) {
				// // System.out.printf(" %d  ", gridCoors[tmpid][k]);
				// // }
				// // System.out.printf("\n");
				// }
				// j++;
				// }
				// }

				// ...........................................................//
			}

			// tmpid = localGridIdx(coordstr);
			// if (gridIdxFlag[tmpid] == 0) {
			// pivotVecAna(pivotstr, pivotVec, tmpid);
			// pivotCoor(coordstr, gridCoors, tmpid);
			// pivotAffs(affRelStr, gridAffs, tmpid);
			// adjIdx(affIdx, gridAdjIdx, tmpid);
			// gridBolt[tmpid] = boltNo;
			// gridPivot[tmpid] = pivotId;
			//
			// gridIdxFlag[tmpid] = 1;
			// }

		}
		return;
	}
}
