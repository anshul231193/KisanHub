import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;


public class DataParser {
	
	
	private static final int BUFFER_SIZE = 4096;
	
	static String outputDir = "E:\\weather.csv";
	
	static LinkedHashSet<String> resultSet = new LinkedHashSet<String>();
	
	static HashMap< Integer, String> months = new HashMap<Integer,String>()
		{{
		     put(1, "JAN");
		     put(2, "FEB");
		     put(3, "MAR");
		     put(4, "APR");
		     put(5, "MAY");
		     put(6, "JUN");
		     put(7, "JUL");
		     put(8, "AUG");
		     put(9, "SEP");
		     put(10, "OCT");
		     put(11, "NOV");
		     put(12, "DEC");
		     put(13, "WIN");
		     put(14, "SPR");
		     put(15, "SUM");
		     put(16, "AUT");
		     put(17, "ANN");
		}};
		
	static HashMap< String, String> temperatures = new HashMap<String,String>()
		{{
		     put("Tmax", "Max Temp");
		     put("Tmean", "Mean Temp");
		     put("Tmin", "Min Temp");
		     put("Sunshine", "Sunshine");
		     put("Rainfall", "Rainfall");
		}};
	
	static String url = "https://www.metoffice.gov.uk/climate/uk/summaries/datasets#yearOrdered";
	
	static String[] countries={"UK","England","Wales","Scotland"};
	
	static String directory = "E:\\KisanHub";

    public static void main(String[] args) throws IOException {
        
    	 parseUrl(url,countries);
    	 parseAccumulateData(directory);
    }

	private static void parseAccumulateData(String directoryName) {
		// TODO Auto-generated method stub
		resultSet.add("region_code,weather_param,year, key, value");
		File directory = new File(directoryName);
        //get all the files from a directory
        File[] fList = directory.listFiles();
        for (File file : fList){
            if (file.isFile()){
                parseFile(file.getName());
            }
        }
		writeResultSetFile(resultSet);
	}

	private static void writeResultSetFile(HashSet<String> resultSet) {
		// TODO Auto-generated method stub
		PrintWriter pw = null;
	    try {
	        pw = new PrintWriter(
	            new OutputStreamWriter(new FileOutputStream(outputDir), "UTF-8"));
	        for (String s : resultSet) {
	            pw.println(s);
	        }
	        pw.flush();
	    } catch (UnsupportedEncodingException | FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
	        pw.close();
	    }
	}

	private static void parseFile(String name) {
		// TODO Auto-generated method stub
		try {
			int i = 0;
			String[] fileSplit = name.split("_");
			String country = fileSplit[0];
			String type = temperatures.get(fileSplit[1]);
			File file = new File(directory+File.separator+name);
			FileReader fileReader = new FileReader(file);
			BufferedReader bufferedReader = new BufferedReader(fileReader);
			StringBuffer stringBuffer = new StringBuffer();
			String line;
			while ((line = bufferedReader.readLine()) != null) {
				if(i>=8){
					line = line.replaceAll("\\s+"," ");
					String[] list = line.split(" ");
					
					for(int k=1;k<list.length;k++){
						String result = "";
						if(list[k].equals("")||list[k]==null)
							result += country+","+type+","+list[0]+","+months.get(k)+","+"N/A";
						else
							result += country+","+type+","+list[0]+","+months.get(k)+","+list[k];
						resultSet.add(result);
					}
				}
				i++;
			}
			fileReader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static void parseUrl(String url, String[] countries) throws IOException {
		// TODO Auto-generated method stub
		Document doc = Jsoup.connect(url).get();  
		for(int i=0;i<countries.length;i++){
	        Elements links = doc.select("a[title^="+countries[i]+" Date]");  
	        for (Element link : links) {  
	            String attr = link.attr("href");
	            if(!(attr.contains("AirFrost") || attr.contains("Raindays1mm"))){
	            	String[] type = attr.split("/");
	            	String fileName = countries[i]+"_"+type[type.length-3];
	            	downloadURLtoFile(attr,directory+File.separator+fileName);
	            }
	        }
		}
	}

	private static void downloadURLtoFile(String fileURL, String saveDir) throws IOException {
		// TODO Auto-generated method stub
		URL url = new URL(fileURL.replace("http", "https"));
        HttpURLConnection httpConn = (HttpURLConnection) url.openConnection();
        int responseCode = httpConn.getResponseCode();
        // always check HTTP response code first
        if (responseCode == HttpURLConnection.HTTP_OK) {
        	// opens input stream from the HTTP connection
            InputStream inputStream = httpConn.getInputStream();
             
            // opens an output stream to save into file
            FileOutputStream outputStream = new FileOutputStream(saveDir);
 
            int bytesRead = -1;
            byte[] buffer = new byte[BUFFER_SIZE];
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                outputStream.write(buffer, 0, bytesRead);
            }
 
            outputStream.close();
            inputStream.close();
 
            System.out.println("File downloaded");
        } else {
            System.out.println("No file to download. Server replied HTTP code: " + responseCode);
        }
        httpConn.disconnect();
    }
	

}
