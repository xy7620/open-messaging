package test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @author XF
 * 测试写入读入在多线程情况下，操作一个文件快还是多个文件。
 */
public class TestMultiFilesPerformance {

	public static void main(String[] args) throws InterruptedException {
		// TODO Auto-generated method stub
		for(int i=0; i<1000; i++){
			writeStr.add("TestMultiFilesPerformance test string "+i);
		}
		TestMultiFilesPerformance tmfp = new TestMultiFilesPerformance();
		Runnable wm = tmfp.writeMulti;
		Runnable ws = tmfp.writeSingle;
		startTime = System.currentTimeMillis();
		
		//测试一个文件快还是多个文件
		for(int i=0; i<20; i++){
			//每个线程写一个文件。共180ms左右
			Thread tm = new Thread(wm);
			tm.start();
			//所有线程写一个文件。共230ms左右
			Thread ts = new Thread(ws);
			ts.start();
		}
		Thread.sleep(1000);
		System.out.println("每个线程分开写最大线程耗时(ms)： "+mTime);
		System.out.println("所有线程写同一个文件耗时(ms)： "+sTime);

	}
	
	private static long startTime;
	private static long mTime;
	private static long sTime;
	private static List<String> writeStr = new ArrayList<String>(1000);
	
	/**
	 * 每个线程写一个文件。
	 */
	private Runnable writeMulti = new Runnable() {
		
		@Override
		public void run() {
			// TODO Auto-generated method stub
			String fileName = Thread.currentThread().getName();
			File file = new File("test/"+fileName+".txt");
			File test = new File("test");
			if(!test.isDirectory()){
				test.mkdir();
			}
			if(!file.isFile()){
				try {
					file.createNewFile();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			try {
				ObjectOutputStream os = new ObjectOutputStream(new FileOutputStream(file));
				os.writeObject(writeStr);
				os.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}finally{
				long endTime = System.currentTimeMillis();
				if(endTime - startTime> mTime){
					mTime = endTime - startTime;
				}
			}
		}
	};
	/**
	 * 所有线程写同一个文件。<p>
	 * 一个更严重的问题是多个线程写一个文件，不加锁的话写入顺序完全不可控。
	 */
	private Runnable writeSingle = new Runnable() {
		
		@Override
		public void run() {
			// TODO Auto-generated method stub
			File file = new File("test/writeSingle.txt");
			if(!file.isFile()){
				try {
					file.createNewFile();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			try {
				ObjectOutputStream os = new ObjectOutputStream(new FileOutputStream(file,true));
				os.writeObject(writeStr);
				os.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}finally{
				long endTime = System.currentTimeMillis();
				sTime = endTime - startTime;
				if(endTime - startTime> sTime){
					sTime = endTime - startTime;
				}
			}
		}
	};

}
