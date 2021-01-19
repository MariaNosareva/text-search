import org.apache.commons.lang3.StringUtils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class TextProcessing {

    private static final int BATCH_SIZE = 1000000;
    private static final int ALPHABET_SIZE = 65536;

    public static void main(String[] args) {
        readFileAndProcess("src/main/resources/fileText.txt");
    }

    public static void readFileAndProcess(String fileName)  {
        Path path = Paths.get(fileName);

        try (BufferedReader reader = Files.newBufferedReader(path)) {
            String line;
            int size = 0;
            int batchNumber = 0;
            StringBuilder batchBuilder = new StringBuilder();

            while ((line = reader.readLine()) != null) {
                size++;
                batchBuilder.append(line);

                if (size == BATCH_SIZE) {
                    String batch = batchBuilder.toString();
                    List<Integer> processedBatch = processBatch(batch);
                    sinkBatch(batchNumber, batch, processedBatch);

                    batchBuilder.setLength(0);
                    batchNumber++;
                    size = 0;
                }
            }

            if (batchBuilder.length() > 0) {
                String batch = batchBuilder.toString();
                List<Integer> processedBatch = processBatch(batch);
                sinkBatch(batchNumber, batch, processedBatch);
            }

        } catch (IOException ex) {
            System.out.println(ex.getMessage());
        }
    }

    public static void sinkBatch(int batchNumber, String batch, List<Integer> processedBatch) {
        String pathText = String.format("%s_text", batchNumber);
        String pathArray = String.format("%s_array", batchNumber);

        writeToFile(batch, pathText);
        writeToFile(StringUtils.join(processedBatch, " "), pathArray);
    }

    public static void writeToFile(String data, String fileName) {
        Path path = Paths.get(fileName);

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName))) {
            writer.write(data);
            writer.close();
        } catch (IOException ex) {
            System.out.println(ex.getMessage());
        }
    }

    public static List<Integer> processBatch(String batch) {
        batch = batch + "\u0000";
        List<Integer> suffArray = new ArrayList<>(Collections.nCopies(batch.length() - 1, 0));

        int numOfClasses = 0;
        List<Integer> suffIndices = new ArrayList<>(Collections.nCopies(batch.length(), 0));
        List<Integer> classes = new ArrayList<>(Collections.nCopies(batch.length(), 0));
        List<Integer> counter = new ArrayList<>(Collections.nCopies(ALPHABET_SIZE, 0));

        for (int i = 0; i < batch.length(); i++) {
            int x = batch.charAt(i);
            counter.set(x, counter.get(x) + 1);
        }

        for (int i = 1; i < ALPHABET_SIZE; i++) {
            counter.set(i, counter.get(i) + counter.get(i - 1));
        }

        for (int i = batch.length() - 1; i >= 0; i--) {
            int x = batch.charAt(i);
            counter.set(x, counter.get(x) - 1);
            suffIndices.set(counter.get(x), i);
        }

        classes.set(suffIndices.get(0), 0);
        numOfClasses++;

        for (int i = 1; i < batch.length(); i++) {
            if (batch.charAt(suffIndices.get(i)) != batch.charAt(suffIndices.get(i - 1))) {
                ++numOfClasses;
            }
            classes.set(suffIndices.get(i), numOfClasses - 1);
        }

        int length = 1;
        while (length < batch.length()) {
            List<Integer> suffIndices2 = new ArrayList<>(Collections.nCopies(batch.length(), 0));
            List<Integer> classes2 = new ArrayList<>(Collections.nCopies(batch.length(), 0));
            counter = new ArrayList<>(Collections.nCopies(numOfClasses, 0));

            for (int i = 0; i < batch.length(); i++) {
                int x = classes.get(i);
                counter.set(x, counter.get(x) + 1);
            }

            for (int i = 1; i < numOfClasses; i++) {
                counter.set(i, counter.get(i) + counter.get(i - 1));
            }

            for (int i = batch.length() - 1; i >= 0; i--) {
                int bigIndex = (suffIndices.get(i) - length + batch.length()) % batch.length();
                int x = classes.get(bigIndex);
                counter.set(x, counter.get(x) - 1);
                suffIndices2.set(counter.get(x), bigIndex);
            }

            suffIndices = suffIndices2;
            classes2.set(suffIndices.get(0), 0);
            numOfClasses = 1;
            for (int i = 1; i < batch.length(); i++) {
                if (classes.get(suffIndices.get(i)) != classes.get(suffIndices.get(i - 1)) ||
                    classes.get((suffIndices.get(i) + length) % batch.length()) != classes.get((suffIndices.get(i - 1) + length) % batch.length())) {
                    ++numOfClasses;
                }
                classes2.set(suffIndices.get(i), numOfClasses - 1);
            }

            classes = classes2;
            length *= 2;
        }
        for (int i = 1; i < suffIndices.size(); i++) {
            suffArray.set(i - 1, suffIndices.get(i));
        }
        return suffArray;
    }
}
