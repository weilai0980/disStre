package grouping;

import java.util.Arrays;
import java.util.List;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.TopologyContext;
import backtype.storm.task.WorkerTopologyContext;
import backtype.storm.tuple.Fields;

public class testgroup implements CustomStreamGrouping {
	
	
	
//	private Map _map;  
    private TopologyContext _ctx;  
    private Fields _fields;  
    private List<Integer> _tasks;
	
    public int tupcnt=0;
    public static int groupid=0;
    public int localgid=0;
    
    
	

	@Override
	public void prepare(WorkerTopologyContext context, GlobalStreamId stream,
			List<Integer> targetTasks) {
		// TODO Auto-generated method stub
		
		_tasks=targetTasks;
		
		localgid=groupid;
		groupid++;
		
		System.out.printf("$$$$$$$$$$$$$  current groupers: %d\n", groupid);
		
	}

	
    int coorAna(String str)
    {
    	int l=str.length();
    	int pre=0,tmpcoor=0;
    	
    	for(int i=0;i<l;++i)
    	{
    		if(str.charAt(i)==',')
    		{
    			tmpcoor=Integer.valueOf( str.substring(pre,i) );
    			pre=i+1;
    		}
    	}
    	return l;
    }
	
	@Override
	public List<Integer> chooseTasks(int taskId, List<Object> values) {
		// TODO Auto-generated method stub
		
		tupcnt++;
		
//		declarer.declareStream("interStre",new Fields("pidx","pivotvec", "adjaffine", "adjidx","coord","ts"));
		
//		System.out.printf("@@@@@@@@@  current tuples: %d\n", tupcnt);
		String tmpcoor=values.get(4).toString();
		int len=coorAna(tmpcoor);
		for(int i=0;i<100;++i)
		{
			for(int j=0;j<len;j++)
			{
				
			}
		}
		
		
		
		long key=Long.valueOf( values.get(0).toString());
		int idx= (int)(key%_tasks.size());
		return Arrays.asList(_tasks.get(idx));
			
//		return null;
	}  

}
