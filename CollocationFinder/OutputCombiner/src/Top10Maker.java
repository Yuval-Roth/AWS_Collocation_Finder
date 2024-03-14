import java.io.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class Top10Maker {

    public static void main (String[] args) throws Exception{

        if(args.length != 1){
            System.out.println("Usage: <threshold>");
            return;
        }

        Map<String,Integer> decadesCounter = new HashMap();
        TreeMap<String,String> map = new TreeMap<>(new Comparator());

        String folderPath = getFolderPath();
        String[] itemsInFolder = Arrays.stream(new File(folderPath).list())
                .filter(s -> ! s.startsWith("Top10Maker") && ! s.startsWith("combined_output"))
                .toArray(String[]::new);

        double threshold = Double.parseDouble(args[0]);

        for(String item : itemsInFolder){
            try(BufferedReader reader = new BufferedReader(new FileReader(folderPath + item))){
                String line;
                while((line = reader.readLine()) != null){
                    String[] tokens = line.split(" ");
                    if(Double.parseDouble(tokens[3]) > threshold) continue;
                    if(tokens[1].equals(tokens[2])) continue;
                    int count = decadesCounter.getOrDefault(tokens[0],0);
                    if(count > 10){
                        continue;
                    }
                    decadesCounter.put(tokens[0],count+1);
                    map.put(line, "");
                }
            }
        }

        try(BufferedWriter writer = new BufferedWriter(new FileWriter(folderPath + "combined_output"))) {
            for(String key : map.keySet()){
                writer.write(key);
                writer.newLine();
            }
        }
    }


    private static String getFolderPath() {
        String folderPath = Top10Maker.class.getResource("Top10Maker.class").getPath();
        folderPath = folderPath.replace("%20"," "); //fix space character
        folderPath = folderPath.substring(folderPath.indexOf("/")+1); // remove initial '/'
        folderPath = folderPath.substring(0,folderPath.lastIndexOf("/")); // remove .class file from path
        folderPath = folderPath.substring(0,folderPath.lastIndexOf("/")+1); // exit jar
        return folderPath;
    }

    public static class Comparator implements java.util.Comparator<String> {

        private static final int DECADE_INDEX = 0;
        private static final int W1_INDEX = 1;
        private static final int W2_INDEX = 2;
        private static final int NPMI_INDEX = 3;


        @Override
        public int compare(String a, String b) {
            String[] aTokens = a.toString().split(" ");
            String[] bTokens = b.toString().split(" ");
            int num;
            if((num = aTokens[DECADE_INDEX].compareTo(bTokens[DECADE_INDEX])) != 0){
                return num;
            }
            else if ((num = Double.valueOf(aTokens[NPMI_INDEX]).compareTo(Double.valueOf(bTokens[NPMI_INDEX]))) != 0){
                return -1 * num;
            }
            else if ((num = aTokens[W1_INDEX].compareTo(bTokens[W1_INDEX])) != 0){
                return num;
            }
            else {
                return aTokens[W2_INDEX].compareTo(bTokens[W2_INDEX]);
            }
        }
    }
}
