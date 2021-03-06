package main;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;


public class RHPbuild {
	
	FileWriter fstream; // =new FileWriter("", true);
	BufferedWriter out; // = new BufferedWriter(fstream);

	
	Random rnd = new Random();
	

	public RHPbuild()
	{
		try {
			fstream = new FileWriter("rhp-vectors.txt", false);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return;
	}
	public void produce(int l, int k, int d) throws IOException // l: # of hash tables; k: dimension of each bitmap; d: dimension of original vector
	{
		
		fstream = new FileWriter(TopologyMain.rp_matFile, true);
		BufferedWriter out = new BufferedWriter(fstream);
		
		for(int i=0;i<l;++i)
		{
			for(int j=0;j<k;++j)
			{
				for(int m=0;m<d;++m)
				{	
					out.write(Double.toString(rnd.nextGaussian())+",");
				}
				out.write("\n");
			}
		}
		
		out.write("\n");
		out.close();
		return;
	}
	
	

}
