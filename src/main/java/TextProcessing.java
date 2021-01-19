import org.apache.commons.lang3.StringUtils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

public class TextProcessing {

    private static final int BATCH_SIZE = 100000;
    private static final int ALPHABET_SIZE = 65536;
    private static int batchCount = 0;
    private static ArrayList<BigInteger> batchSizes = new ArrayList<>(Collections.nCopies(1, BigInteger.valueOf(0)));

    private static final String TEXT_FILENAME_PATTERN = "/tmp/%s_text";
    private static final String ARRAY_FILENAME_PATTERN = "/tmp/%s_array";
    private static final String INITIAL_FILE = "/tmp/fileText.txt";

    public static void main(String[] args) {
        readFileAndProcess(INITIAL_FILE);

        Scanner scanner = new Scanner(System.in);
        System.out.println("Input a pattern: ");
        while (true) {
            String pattern = scanner.next();
            if (pattern.equals(":q!")) {
                System.out.println("Bye :)");
                break;
            }

            BigInteger index = searchPattern(pattern);
            System.out.println(String.format("position: %s", index.signum() >= 0 ? index : "not found"));
            System.out.println("And another one: ");
        }
    }

    public static BigInteger searchPattern(String pattern) {

        for (int i = 0; i < batchCount; i++) {
            String textFileName = String.format(TEXT_FILENAME_PATTERN, i);
            String arrayFileName = String.format(ARRAY_FILENAME_PATTERN, i);

            Path textPath = Paths.get(textFileName);
            Path arrayPath = Paths.get(arrayFileName);

            try (BufferedReader textReader = Files.newBufferedReader(textPath);
                 BufferedReader arrayReader = Files.newBufferedReader(arrayPath)) {

                String batch = textReader.readLine();
                String suffArray = arrayReader.readLine();

                List<Integer> array = Arrays.stream(suffArray.split(" "))
                        .map(Integer::parseInt)
                        .collect(Collectors.toList());

                int index = findInArray(pattern, batch, array);
                if (index >= 0) {
                    return batchSizes.get(i).add(BigInteger.valueOf(index));
                }
            } catch (IOException ex) {
                System.out.println(ex.getMessage());
            }
        }
        return BigInteger.valueOf(-1);
    }

    public static int findInArray(String pattern, String batch, List<Integer> suffArray) {
        TextComparator comparator = new TextComparator(batch, pattern);
        int index = Collections.binarySearch(suffArray, pattern.length(), comparator);
        return index < 0 ? -1 : suffArray.get(index);
    }

    static class TextComparator implements Comparator<Integer> {
        private String text;
        private String pattern;

        public TextComparator(String text, String pattern) {
            this.text = text;
            this.pattern = pattern;
        }

        @Override
        public int compare(Integer o1, Integer o2) {
            return StringUtils.compare(text.substring(o1, Math.min(o1 + o2, text.length())), pattern);
        }
    }

    public static void readFileAndProcess(String fileName)  {
        Path path = Paths.get(fileName);

        try (BufferedReader reader = Files.newBufferedReader(path)) {
            String line;
            int size = 0;
            int batchNumber = 0;
            StringBuilder newBatchBuilder = new StringBuilder();
            StringBuilder oldBatchBuilder = new StringBuilder();

            while ((line = reader.readLine()) != null) {
                size++;
                newBatchBuilder.append(line);

                if (size == BATCH_SIZE / 2) {
                    if (oldBatchBuilder.length() > 0) {
                        process(oldBatchBuilder.toString() + newBatchBuilder.toString(), batchNumber);
                    }

                    batchSizes.add(BigInteger.valueOf(newBatchBuilder.length()).add(batchSizes.get(batchSizes.size() - 1)));
                    batchNumber++;
                    batchCount = batchNumber;
                    oldBatchBuilder = new StringBuilder(newBatchBuilder);
                    newBatchBuilder.setLength(0);
                    size = 0;
                }
            }

            if (newBatchBuilder.length() > 0) {
                if (oldBatchBuilder.length() > 0) {
                    process(oldBatchBuilder.toString() + newBatchBuilder.toString(), batchNumber);
                } else {
                    process(newBatchBuilder.toString(), batchNumber);
                }
                batchCount++;
            }

        } catch (IOException ex) {
            System.out.println(ex.getMessage());
        }
    }

    public static void process(String batch, int batchNumber) {
        List<Integer> processedBatch = processBatch(batch);
        sinkBatch(batchNumber, batch, processedBatch);
    }

    public static void sinkBatch(int batchNumber, String batch, List<Integer> processedBatch) {
        String pathText = String.format(TEXT_FILENAME_PATTERN, batchNumber);
        String pathArray = String.format(ARRAY_FILENAME_PATTERN, batchNumber);

        writeToFile(batch, pathText);
        writeToFile(StringUtils.join(processedBatch, " "), pathArray);
    }

    public static void writeToFile(String data, String fileName) {
        Path path = Paths.get(fileName);

        try (BufferedWriter writer = Files.newBufferedWriter(path)) {
            writer.write(data);
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
