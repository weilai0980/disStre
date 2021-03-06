package main;

import spouts.*;

import bolts.*;
import grouping.*;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import java.io.*;
import java.util.ArrayList;

public class TopologyMain {

	// .................local parallelism record...........................//
	public static int datasrc = 0; // 0: synthetic 1: real
	
	
	 public static int nstreBolt = 20;
	 public static double thre = 0.9;
	 public static int nstream = 20;
	 public static int winSize = 3;
	 public static int gridIdxN = 500;
//	
	 public static int tinterval = 1000;
	 public static int wokernum = 2;
	
	 public static int preBoltNum = 2;
	 public static int calBoltNum = 8;
	 public static int aggreBoltNum = 1;
	
	public static int tasknum = 2;


	// ......................................................................//
	
	
	
	// real data

//	 public static int nstreBolt = 30;
//	 public static double thre = 0.98;
//	 public static int nstream = 20;
//	 public static int winSize = 3;
//	 public static int gridIdxN=1000;
	
	
//	 public static int nstrFile = 5;
//	 public static int offsetRow = 0;
	 
	 public static int nstrFile = 20;
	 public static int offsetRow = 0;
	 public static int sampRate=10000;

	// ............cluster parameter ..............//

//	public static int sampRate=10000;
//		
//	public static int tinterval = 5000;
//	public static int winSize = 10; // 9 12 15 18: 2500 * 10 20  40 80 160 320 640
//	public static double thre = 0.95; // 0.95 0.9 0.85 0.8 *
//
//	public static int wokernum = 16;
//	public static int preBoltNum = 16; //58 118
//	public static int calBoltNum = 16; // 58 118 
//	public static int aggreBoltNum = 2;
//	
	public static int winh= (int) (Math.log(calBoltNum) / Math.log(2));
//	
//	public static int NUM=1000; // naive: 1400, gbc: 2400, aps: 80000
//	public static int nstreBolt = NUM;
//	public static int nstream = NUM;
//	public static int gridIdxN = NUM;
//	
//	public static int datasrc = 0; // 0: synthetic 1: real

	
//	ti:   naive 1400 2300 3000s
//	     gbc 2400 3500 4500
//	     aps 8000  10000 12000
	
	
//	p:  naive 1500 1700 1850
//      gbc 2750  2850
//      aps 9000 9800
	
	
//	epsilon:  
//	naive: 1450 1460 1490    
//  gbc: 2700 2820 2970  
//  aps:  9300 9650 9850 
	
// window:  
//		naive: 1350 1240 1160(1130)  1050 1000 930 830
	//  gbc:   
	//  aps: 7600
	
	

	

	// .................test..........................//

	// static ArrayList<Integer> emitStack= new ArrayList<Integer>();
	// public static int[] direcVec = { -1, 0, 1 };
	//
	// static void gridEmitNoRecur(int orgiCoord[]) {
	//
	// int curlay = 0;
	// int[] tmpCoor = new int[TopologyMain.winSize + 5];
	// String coordstr = new String();
	//
	// int stkSize = 0, curdir = 0;
	// int tmpdir = 0;
	//
	// // .....ini....................//
	// emitStack.add(curdir);
	// stkSize++;
	// tmpdir = curdir;
	//
	// tmpCoor[curlay] = orgiCoord[curlay] + direcVec[tmpdir];
	// if (tmpCoor[curlay] == 0) {
	// if (tmpdir == 0) {
	// tmpCoor[curlay] = -1;
	// } else if (tmpdir == 2) {
	// tmpCoor[curlay] = 1;
	// }
	// }
	//
	// curlay++;
	// curdir = -1;
	// // ...........................//
	//
	// while (emitStack.size() != 0) {
	// if (curlay >= 3) {
	//
	// coordstr = "";
	// for (int i = 0; i < 3; ++i) {
	// coordstr = coordstr + Integer.toString(tmpCoor[i]) + ",";
	//
	// // if (tmpCoor[i] > boundCell)
	// // break;
	// // if (tmpCoor[i] < -1 * boundCell)
	// // break;
	// //
	// // adjCell.add(coordstr);
	//
	//
	//
	// }
	// System.out.printf("%s\n",coordstr);
	//
	// curdir = emitStack.get(stkSize - 1);
	// emitStack.remove(stkSize - 1);
	// stkSize--;
	// curlay--;
	//
	// } else {
	//
	// if (curdir+1 > 2) {
	//
	// curdir = emitStack.get(stkSize - 1);
	// emitStack.remove(stkSize - 1);
	// stkSize--;
	// curlay--;
	//
	// continue;
	// } else {
	//
	// emitStack.add(curdir + 1);
	// stkSize++;
	//
	// tmpdir = curdir + 1;
	// tmpCoor[curlay] = orgiCoord[curlay] + direcVec[tmpdir];
	// if (tmpCoor[curlay] == 0) {
	// if (tmpdir == 0) {
	// tmpCoor[curlay] = -1;
	// } else if (tmpdir == 2) {
	// tmpCoor[curlay] = 1;
	// }
	// }
	//
	// curlay++;
	// curdir = -1;
	//
	// }
	// }
	// }
	//
	// return;
	// }

	// ...............................................//

	public static void main(String[] args) throws InterruptedException,
			IOException, AlreadyAliveException, InvalidTopologyException {

		System.out.printf("i am ok\n");
		int appro = Integer.valueOf(args[0]);

		// //............group test...................//
		// TopologyBuilder builderNavie = new TopologyBuilder();
		// builderNavie.setSpout("sreader", new streamReader());
		//
		// builderNavie.setBolt("test", new testGroupBolt(),
		// 4).customGrouping("sreader", new testgroup());
		// builderNavie.setBolt("test1", new naivePreBolt(),
		// 4).customGrouping("sreader", new testgroup());

		// .shuffleGrouping("sreader");

		// Configuration
		Config conf = new Config();
//		conf.put("steamsFile", "/root/guo/srcStorm/dataset/"); // needs modify
		
		conf.put("steamsFile", "/home/guo/conqry/streamqry/dataset/");
		
//		conf.put("steamsFilter", "part");
		conf.put("steamsFilter", "syn"); // needs modify
		// conf.put("steamsFilter", "A"); // needs modify
		conf.setDebug(false);

		conf.setNumWorkers(wokernum);

		// Topology run
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		LocalCluster cluster = new LocalCluster();

		// curApp=appro;

		if (appro == 0) {

			String runenv = args[1];

			TopologyBuilder builderNavie = new TopologyBuilder();
			builderNavie.setSpout("sreader", new streamReader());

			builderNavie.setBolt("naviepre", new naivePreBolt(), preBoltNum)
					.fieldsGrouping("sreader", new Fields("sn"));
			builderNavie.setBolt("naviestatis", new naiveStatisBolt(),
					calBoltNum)
					.fieldsGrouping("naviepre", new Fields("tspair"));
			builderNavie.setBolt("navieaggre", new naiveAggreBolt(),
					aggreBoltNum).fieldsGrouping("naviestatis",
					new Fields("taskid"));

			// taskid
			// .fieldsGrouping("naviestatis", new Fields("pair"));

			if (runenv.compareTo("local") == 0) {
				cluster.submitTopology("strqry", conf,
						builderNavie.createTopology());
				Thread.sleep(2000);
			} else if (runenv.compareTo("cluster") == 0) {
				StormSubmitter.submitTopology("conqry", conf,
						builderNavie.createTopology());
			}

		} else if (appro == 1) { // may add hashset or hashmap to boost the
									// performance

			// .................test.............................//

			// int tmp[]={1,2,3};
			// gridEmitNoRecur(tmp);
			//
			// ..................................................//

			String runenv = args[1];

			TopologyBuilder builderGrid = new TopologyBuilder();
			builderGrid.setSpout("sreader", new streamReader());

			builderGrid.setBolt("gridpre", new gridPreBolt(), preBoltNum)
					.fieldsGrouping("sreader", new Fields("sn"));

			builderGrid.setBolt("gridstatis", new gridStatisBolt(), calBoltNum)
					.fieldsGrouping("gridpre", new Fields("coord"));

			builderGrid.setBolt("gridaggre", new gridAggreBolt(), aggreBoltNum)
					.fieldsGrouping("gridstatis", new Fields("pair"));

			if (runenv.compareTo("local") == 0) {
				cluster.submitTopology("strqry", conf,
						builderGrid.createTopology());
				Thread.sleep(2000);
			} else if (runenv.compareTo("cluster") == 0) {
				StormSubmitter.submitTopology("conqry", conf,
						builderGrid.createTopology());
			}

		}

		else if (appro == 2) { // may add hashset or hashmap to boost the
								// performance

			// .....................test.................................//

			//
			// double diviNum = Math.exp(Math.log(TopologyMain.calBoltNum)
			// / TopologyMain.winSize); // division on each dimension
			//
			// double taskRange = 2.0 / diviNum;
			// double disThre = 2 - 2 * TopologyMain.thre;
			//
			// int gridRange = (int) Math.ceil(1.0 / Math.sqrt(disThre));
			// // public int taskGridMax=2*gridRange-1;
			//
			// double taskGridCap = taskRange / Math.sqrt(disThre);
			//
			//
			//
			// System.out.printf("%f  %f  %f %d %f \n",
			// diviNum,taskRange,disThre,gridRange, taskGridCap);

			// ...........................................................//

			String runenv = args[1];

			TopologyBuilder builderAdjust = new TopologyBuilder();
			builderAdjust.setSpout("sreader", new streamReader());

			builderAdjust.setBolt("adjPre", new AdjustPreBolt(), preBoltNum)
					.fieldsGrouping("sreader", new Fields("sn"));

			builderAdjust
					.setBolt("adjAppro", new AdjustApproBolt(), calBoltNum)
					.customGrouping("adjPre", "interStre",
							new AdjustHashGroupEnh());

			builderAdjust
					.setBolt("adjAggre", new AdjustAggreBolt(), aggreBoltNum)
					.fieldsGrouping("adjPre", "qualStre", new Fields("pair"))
					.fieldsGrouping("adjAppro", "interQualStre",
							new Fields("pair"));

			if (runenv.compareTo("local") == 0) {
				cluster.submitTopology("strqry", conf,
						builderAdjust.createTopology());
				Thread.sleep(2000);
			} else if (runenv.compareTo("cluster") == 0) {
				StormSubmitter.submitTopology("conqry", conf,
						builderAdjust.createTopology());
			}

		} else if (appro == 10) {

			dataGene dataProd = new dataGene();
			dataProd.generate();
			System.out.printf("!!   Data set is produced\n");
			// System.out.printf("%f   %f\n", Math.ceil(-1.3),Math.ceil(1.3));
		} else if (appro == 20) {

			int sampcnt = 0, recFlag=1;
			int curApp = Integer.valueOf(args[1]);
			
			

			while (sampcnt < 10) {

				Thread.sleep(sampRate);

				ClusterInformationExtractor metrics = new ClusterInformationExtractor();
				metrics.infoExtractor(curApp, recFlag);
				sampcnt++;
				
				recFlag=0;
			}
		}

		System.out.printf("topology is finishing");
		cluster.shutdown();

	}
}
