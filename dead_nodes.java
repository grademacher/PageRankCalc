import java.io.*;
import java.nio.file.*;
import java.util.stream.Stream;

public class dead_nodes{

    public static void main(String[] args) throws IOException{
        //create output file writer
        FileWriter output_writer = new FileWriter("dead_nodes.txt");

        //create the input stream
        Stream<String> stream = Files.lines(Paths.get("wiki-topcats.txt"));
        //write to the file
        stream.filter(line -> line.split(" ").length == 1).forEach(line -> printToFile(line, output_writer));

        //close the files and stream
        stream.close();
        output_writer.close();

    }

    public static void printToFile(String line, FileWriter fw){
        //write line to a file
        try{
            fw.write(line + "\n");
        }catch(IOException e){
            e.printStackTrace();
        }
    }
}
