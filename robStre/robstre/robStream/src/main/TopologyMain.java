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

	
	// .................DFT approach...........................//
	
	public static int dftN = 4;
	
	//.................Random projection......................//
	
	public static int vecnum=10;
	public static int dimnum=10;
	
	
	// .................local parallelism record...........................//
	public static int datasrc = 1; // 0: synthetic 1: real
	public static int nstreBolt = 20;
	public static int nstream = 20;
	public static int gridIdxN = 500;

	
	public static int winSize = 3;
	public static double thre = 0.9;
	public static int tinterval = 1000;
	public static int wokernum = 2;

	public static int preBoltNum = 2;
	public static int calBoltNum = 8;
	public static int aggreBoltNum = 1;

	public static int tasknum = 2;
	public static int cellTask = 2;

	// ..............real data set para................................//

	// public static int nstreBolt = 30;
	// public static double thre = 0.98;
	// public static int nstream = 20;
	// public static int winSize = 3;
	// public static int gridIdxN=1000;

	public static int nstrFile = 20;
	public static int offsetRow = 0;

	// ............cluster parameter ..............//

	public static int sampRate = 10000;
	//
	// public static int tinterval = 5000;
	// public static int winSize = 10; // 9 12 15 18: 2500 * 10 20 40 80 160 320
	// 640
	// public static double thre = 0.95; // 0.95 0.9 0.85 0.8 *
	//
	// public static int wokernum = 30;
	// public static int preBoltNum = 32; //58 118
	// public static int calBoltNum = 128; // 58 118
	// public static int aggreBoltNum = 30;
	//
	public static int winh = (int) (Math.log(calBoltNum) / Math.log(2));

	//
	// public static int NUM=1000; // naive: 1400, gbc: 2400, aps: 80000
	// public static int nstreBolt = NUM;
	// public static int nstream = NUM;
	// public static int gridIdxN = NUM;
	//
	// public static int datasrc = 0; // 0: synthetic 1: real

	// ---------------------rough results-------------------------------//

	// ti: naive 1400 2300 3000s
	// gbc 2400 3500 4500
	// aps 8000 10000 12000

	// p: naive 1500 1700 1850
	// gbc 2750 2850
	// aps 9000 9800

	// epsilon:
	// naive: 1450 1460 1490
	// gbc: 2700 2820 2970
	// aps: 9300 9650 9850

	// window:
	// naive: 1350 1240 1160(1130) 1050 1000 930 830
	// gbc:
	// aps: 7600

	public static void main(String[] args) throws InterruptedException,
			IOException, AlreadyAliveException, InvalidTopologyException {

		System.out.printf("i am ok\n");
		int appro = Integer.valueOf(args[0]);

		// Configuration
		Config conf = new Config();
		conf.put("steamsFile", "/home/guo/conqry/streamqry/dataset/");
		conf.put("steamsFilter", "syn"); // needs modify

		conf.setDebug(false);
		conf.setNumWorkers(wokernum);

		// Topology run
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		LocalCluster cluster = new LocalCluster();

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

			if (runenv.compareTo("local") == 0) {
				cluster.submitTopology("strqry", conf,
						builderNavie.createTopology());
				Thread.sleep(2000);
			} else if (runenv.compareTo("cluster") == 0) {
				StormSubmitter.submitTopology("conqry", conf,
						builderNavie.createTopology());
			}

		} else if (appro == 1) {

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

		else if (appro == 2) {

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

		} else if (appro == 3) {

			String runenv = args[1];

			TopologyBuilder builderRob = new TopologyBuilder();
			builderRob.setSpout("sreader", new streamReader());

			builderRob.setBolt("robPre", new robPreBolt(), preBoltNum)
					.fieldsGrouping("sreader", "dataStre", new Fields("sn"))
					.allGrouping("sreader", "contrStre");

			builderRob
					.setBolt("robCal", new robCalBolt(), calBoltNum)
					.customGrouping("robPre", "streamData", new robStripGroup())
					.allGrouping("robPre", "calCommand");

//			builderRob.setBolt("robAggre", new robAggreBolt(), calBoltNum)
//					.fieldsGrouping("robCal", new Fields("pair"));

			if (runenv.compareTo("local") == 0) {
				cluster.submitTopology("strqry", conf,
						builderRob.createTopology());
				Thread.sleep(2000);
			} else if (runenv.compareTo("cluster") == 0) {
				StormSubmitter.submitTopology("conqry", conf,
						builderRob.createTopology());
			}

		}

		else if (appro == 10) {

			dataGene dataProd = new dataGene();
			dataProd.generate();
			System.out.printf("!!   Data set is produced\n");
		} else if (appro == 20) {

			int sampcnt = 0, recFlag = 1;
			int curApp = Integer.valueOf(args[1]);

			while (sampcnt < 10) {

				Thread.sleep(sampRate);

				ClusterInformationExtractor metrics = new ClusterInformationExtractor();
				metrics.infoExtractor(curApp, recFlag);
				sampcnt++;

				recFlag = 0;
			}
		}

		System.out.printf("topology is finishing");
		cluster.shutdown();

	}
}