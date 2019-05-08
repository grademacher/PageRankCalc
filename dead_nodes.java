import java.io.*;
import java.nio.file.*;
import java.util.stream.Stream;

public class dead_nodes{
  //file vars
  private String input_file = "";
  private String output_file = "";

  public static void main(String[] args) throws IOException{
    //create output file writer
    File output_file = new File(output_file);
    FileWriter output_writer = new FileWriter(output_file);

    //create the input stream
    Stream<Stream> stream = File.lines(Path.get(input_file));
    stream.filter(line -> !(line.split(" ").length > 1)).forEach(line -> printToFile(line));

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
