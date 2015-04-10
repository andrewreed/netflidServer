import java.util.*;
import java.io.*;
import java.net.*;
import java.math.*;
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;
import org.apache.commons.math3.stat.StatUtils;
import net.sf.javaml.core.kdtree.KDTree;

public class ServerThread implements Runnable {

	private Socket clientSocket;
	private KDTree windowDB;

	public static final int WINDOW_SIZE = 30;

	public ServerThread(Socket clientSocket, KDTree windowDB) {
		this.clientSocket = clientSocket;
		this.windowDB = windowDB;
	}

	@Override
	public void run() {
		PearsonsCorrelation correlator = new PearsonsCorrelation();

		String ipAddr = clientSocket.getInetAddress().toString();

		System.out.println(ipAddr + "\tconnected");

		PrintWriter out = null;
		Scanner in = null;

		try {
			out = new PrintWriter(clientSocket.getOutputStream(), true); 
			in = new Scanner(clientSocket.getInputStream());
		} catch (IOException e) {
			System.err.println(ipAddr + "\treader/writer failed"); 
			return; 
		} 

		while (in.hasNextLine()) {
			String inputLine = in.nextLine();
			String[] tokens = inputLine.split("\\t");

			if (tokens[0].equals("complete")) {
				out.print("\f");
				System.out.println(ipAddr + "\tcomplete");
				break;
			}

			String macAddr = tokens[0];
			String startTime = tokens[1];

			String[] readings = tokens[2].split(",");
			int[] sums = new int[WINDOW_SIZE];

			for (int i = 0; i < WINDOW_SIZE; i++) {
				sums[i] = Integer.parseInt(readings[i]);
			}

			short dummyBitrate = 0;
			Window currentWindow = new Window(startTime, dummyBitrate, 0, sums);
			
			double[] key = currentWindow.getKey();

			double[] lowerKey = new double[]{key[0] * 0.98,
			                                 key[1] - 0.03,
			                                 key[2] - 0.03,
			                                 key[3] - 0.03,
			                                 key[4] - 0.03,
			                                 key[5] - 0.03};

			double[] upperKey = new double[]{key[0] * 1.02,
			                                 key[1] + 0.03,
			                                 key[2] + 0.03,
			                                 key[3] + 0.03,
			                                 key[4] + 0.03,
			                                 key[5] + 0.03};

			Object[] shortList = windowDB.range(lowerKey, upperKey);

			int[] currentSegments = currentWindow.getSegments();

			for (int i = 0; i < shortList.length; i++) {
				Window compareWindow = (Window)shortList[i];
				int compareStart = compareWindow.getStartIndex();
				int[] compareSegments = compareWindow.getSegments();

				double[] currentSnapSamples = new double[24];
				for (int y = 0; y < 24; y++) {
					currentSnapSamples[y] = ((3.0*currentSegments[y]) + 
								                  (-7.0*currentSegments[y+1]) + 
								                   (1.0*currentSegments[y+2]) + 
								                   (6.0*currentSegments[y+3]) + 
								                   (1.0*currentSegments[y+4]) + 
								                  (-7.0*currentSegments[y+5]) + 
								                   (3.0*currentSegments[y+6])) / 11.0;
				}

				double[] compareSnapSamples = new double[24];
				for (int y = 0; y < 24; y++) {
					compareSnapSamples[y] = ((3.0*compareSegments[compareStart + y]) + 
								                  (-7.0*compareSegments[compareStart + y + 1]) + 
								                   (1.0*compareSegments[compareStart + y + 2]) + 
								                   (6.0*compareSegments[compareStart + y + 3]) + 
								                   (1.0*compareSegments[compareStart + y + 4]) + 
								                  (-7.0*compareSegments[compareStart + y + 5]) + 
								                   (3.0*compareSegments[compareStart + y + 6])) / 11.0;
				}

				double snapCorrel = correlator.correlation(currentSnapSamples, compareSnapSamples);

				if (snapCorrel < 0.91) {
					continue;
				}

				double[] currentAsDoubles = new double[30];
				double[] compareAsDoubles = new double[30];

				for (int y = 0; y < 30; y++) {
					currentAsDoubles[y] = (double)currentSegments[y];
					compareAsDoubles[y] = (double)compareSegments[compareStart + y];
				}

				double posCorrel = correlator.correlation(currentAsDoubles, compareAsDoubles);

				if (posCorrel < 0.95) {
					continue;
				}

				out.println(macAddr + "\t" +
				            currentWindow.getTitle() + "\t" + 
				            compareWindow.getTitle() + "\t" + 
				            compareWindow.getStartIndex() + "\t" +
				            compareWindow.getKey()[0] / key[0] + "\t" +
				            snapCorrel + "\t" +
				            posCorrel);
			}
		}
		try {
			out.close();
			in.close();
			clientSocket.close();
		} catch (IOException e) { 
			System.err.println(ipAddr + "\tclean-up failed"); 
		} 
		System.out.println(ipAddr + "\tdisconnected");
	}
}
