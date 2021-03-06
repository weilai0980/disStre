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
import backtype.storm.tuple.Tuple;

public class naiveAggreBolt extends BaseBasicBolt {

	double curtstamp = TopologyMain.winSize-1;

	int rescnt = 0;
	int [] taskIdx=new int[TopologyMain.calBoltNum+2];
	int[] taskids= new int[TopologyMain.calBoltNum+2];
	int taskcnt=0;

	FileWriter fstream; // =new FileWriter("", true);
	BufferedWriter out; // = new BufferedWriter(fstream);


	//................test.............................//
//	int[] strid = new int[TopologyMain.nstream+10];
//	int stridcnt = 0;
//	
////	int[][] pairIdx = new int[TopologyMain.nstream][TopologyMain.nstream];
//	
//	String [] qualPair=new String[TopologyMain.nstream*10];
//	int qualPairCnt=0;
//	
	HashSet<String> pairHset = new HashSet<String>();
	//................................................//
	
//	public int localStreIdx(int streid) {
//		int i = 0;
//		for (i = 0; i < stridcnt; ++i) {
//			if (strid[i] == streid) {
//				break;
//			}
//		}
//		if (i == stridcnt) {
//			strid[stridcnt++] = streid;
//		}
//		return i;
//	}
   
//	public void pairAna(String pair,int pairInt[])
//	{
//		int len=pair.length();
//		for(int i=0;i<len;++i)
//		{
//			if(pair.charAt(i)==',')
//			{
//				pairInt[0]=Integer.valueOf(pair.substring(0, i));
//				pairInt[1]=Integer.valueOf(pair.substring(i+1, len));
//				return;
//			}
//		}
//		return;
//	}
	
//	public void pairAna(String pair,int pairInt[])
//	{
//		int len=pair.length();
//		int cnt=0,pre=0;
//		
////		System.out.printf("%s\n",pair);
//		
//		for(int i=0;i<len;++i)
//		{
//			if(pair.charAt(i)==',')
//			{
//				pairInt[cnt++]=Integer.valueOf(pair.substring(pre, i));
//				pre=i+1;
//				
//				if(cnt==2)
//					return;
//				
////				pairInt[1]=Integer.valueOf(pair.substring(i+1, len));
////				return;
//			}
//		}
//		return;
//	}
	
	
	@Override
	public void cleanup() {
//		try {
//			fstream = new FileWriter("naiveRes.txt", true);
//			BufferedWriter out = new BufferedWriter(fstream);
//			out.close();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
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
//		BufferedWriter out = new BufferedWriter(fstream);
		
		return;
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub

		double ts = input.getDoubleByField("ts");
		int cnt = input.getIntegerByField("num");
		int task = input.getIntegerByField("taskid");
	
		
//		int [] stres= new int[4];
//		int stre1=0,stre2=0;
		
//		String pairstr=input.getStringByField("pair");
//		pairAna(pairstr,stres);

		if (ts > curtstamp) {
		
//				try {
////					fstream = new FileWriter("naiveRes.txt", true);
//					BufferedWriter out = new BufferedWriter(fstream);
//					
//					out.write("Timestamp  "+Double.toString(curtstamp)+", "+"total num  "+Integer.toString(rescnt)+":  ");
//					
////					for(int i=0;i<qualPairCnt;++i)
////					{
////						out.write(qualPair[i]+"   ");
////					}
//					out.write("\n");
//					out.close();
//				} catch (IOException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
	
			
		
//			for(int i=0;i<TopologyMain.nstream;++i)
//			{
//				for(int j=0;j<TopologyMain.nstream;++j)
//				{
//					pairIdx[i][j]set=0;
//				}
//			}
//				qualPairCnt=0;	
//
//			stre1=localStreIdx(stres[0]);
//			stre2=localStreIdx(stres[1]);
//			if(pairIdx[stre1][stre2]==0)
//			{
//				pairIdx[stre1][stre2]=1;
//				pairIdx[stre2][stre1]=1;
//				
//				qualPair[qualPairCnt++]=pairstr;
//				rescnt++;
//			}
			
			
			
			System.out.printf("time stamp %f:   %d\n ",curtstamp,rescnt);
		
//			for (String s : pairHset) {
//			    System.out.println(s+",  ");
//			}
//			System.out.printf("\n");
			
			
//			qualPairCnt=0;
//			pairHset.clear();
//			
//
//			if(pairHset.contains(pairstr)==false)
//			{
//				qualPairCnt++;
//				pairHset.add(pairstr);
//			}
			
			
			//......original update...........//
			
		
			rescnt = 0;		
			curtstamp = ts;	
			

			for(int i=0;i<TopologyMain.calBoltNum;++i)
			{
				taskIdx[i]=0;
			}
			
			if(taskIdx[task]==0)
			{
				rescnt+=cnt;
				taskIdx[task]=1;
			}

		} else if (ts < curtstamp) {
			System.out.printf("!!!!!!!!!!!!! naive aggregate bolt: time sequence disorder\n");
		} else if (Math.abs(ts - curtstamp) <= 1e-3) {

//			stre1=localStreIdx(stres[0]);
//			stre2=localStreIdx(stres[1]);
//			
//			if(pairIdx[stre1][stre2]==0)
//			{					
//				pairIdx[stre1][stre2]=1;
//				pairIdx[stre2][stre1]=1;
//				
//				qualPair[qualPairCnt++]=pairstr;
//				rescnt++;
//			}
//			 
			
//			if(pairHset.contains(pairstr)==false)
//			{
//				qualPairCnt++;
//				pairHset.add(pairstr);
//			}
			
			
			//......original update...........//
			if(taskIdx[task]==0)
			{
				
				
				rescnt+=cnt;
				
//				System.out.printf("time stamp %f:   %d from task %d\n",curtstamp,rescnt,task);
				
				taskIdx[task]=1;
			}
		}
		return;

	}
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
	}
}
