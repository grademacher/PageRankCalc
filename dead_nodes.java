import java.io.*;
import java.nio.file.*;
import java.util.stream.Stream;

public class dead_nodes{
  //file vars
  private String edges_file = "";
  private String node_names_file = "";
  private String output_file = "";
  private Map<Integer, Integer> node_list = new HashMap<Integer, Integer>();

  public static void main(String[] args) throws IOException{
    //create output file writer
    File output_file = new File(output_file);
    FileWriter output_writer = new FileWriter(output_file);

    //create the input stream
<<<<<<< HEAD
    Stream<Stream> stream = File.lines(Path.get(node_names_file));
    stream.forEach(line -> node_list.put(Integer.valueOf(line), 0);

    Stream<Stream> stream = File.lines(Path.get(edges_file));
    stream.forEach(line -> node_list.replace(line.split("")[0], (node_list.get(Integer.valueOf(line.split("")[0]))+1)) );

    for(int i = 0; i < node_list.keySet().size; i++){
      if(node_list.get(node_list.keySet()[i]) == 0){
        printToFile("" + node_list.keySet()[i], output_writer);
      }
    }
=======
    Stream<Stream> stream = File.lines(Path.get(input_file));
    stream.filter(line -> line.split(" ").length == 1).forEach(line -> printToFile(line));
<<<<<<< HEAD
>>>>>>> parent of d16c9bd... Changed Filter
=======
>>>>>>> parent of d16c9bd... Changed Filter

    //close the files and stream
    stream.close();
    output_writer.flush();
    output_writer.close();

  }

  public static void printToFile(String line, FileWriter fw){
    //write line to a file
    try{
      fw.write(line);
    }catch(IOException e){
      e.printStackTrace();
    }
  }
}
