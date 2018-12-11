/*
This class defines the ways we read files.
It is like a "tool" class.
*/

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.ArrayList;
import java.util.List;

public class ReadFile {
    
    // read file line by line
    public static List<String> readByLine(String fileName) {
        
        List<String> lines = new ArrayList<String>();
        String line = null;
        LineNumberReader reader = null;
        
        try {
            // create a reader instance
            reader = new LineNumberReader(new FileReader(fileName));
            // read line by line
            while ((line = reader.readLine()) != null) {
                lines.add(line);
            }
        }
        catch(FileNotFoundException e) {
            e.printStackTrace();
        }
        catch(IOException e) {
            e.printStackTrace();
        }
        finally {
            // close lineNumberReader
            try {
                if(reader != null) {
                    reader.close();
                }
            }
            catch(IOException e) {
                e.printStackTrace();
            }
        }
        
        return lines;
    }
    
}
