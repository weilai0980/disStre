import java.io.IOException;



public class coorGenerationTest {
	
	int[] direcVec = {-1,0,1};
	int[] a = {1,2,3};
	
	void calCellCoor( int dftNum,int curCnt, String cellCoor, int coor[]) {

		if(curCnt>=dftNum)
		{
//			System.out.printf("%s\n", cellCoor);
			
			for(int i=0;i<dftNum;++i)
			{
				System.out.printf("%d", coor[i]);
			}
			System.out.printf("\n");
			
			return;
			
		}
		for(int i=0;i<3;++i)
		{
			cellCoor=cellCoor+ Integer.toString(a[curCnt]+direcVec[i])+",";
			
			coor[curCnt]=a[curCnt]+direcVec[i];
			
			calCellCoor(dftNum, curCnt+1, cellCoor,coor);
		
		}
	
		return;
	}

	public static void main(String[] args)  {
		
		String cell=new String();
		int[] res= new int[10];
		coorGenerationTest obj=new coorGenerationTest();
		obj.calCellCoor( 3,0, cell,res);
	
	}
	

}
