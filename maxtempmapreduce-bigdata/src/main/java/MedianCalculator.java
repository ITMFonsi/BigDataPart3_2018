import UE2.NcdcRecordParser;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class MedianCalculator {

    public NcdcRecordParser parser = new NcdcRecordParser();
    public ArrayList<Double> list = new ArrayList<>();

    public ArrayList<Double> get1901Values() {
        List<String> lines = getInputFromFile("src/test/resources/1901");

        for(String line : lines) {
            parser.parse(line);
            if (parser.isValidTemperature()) {
                list.add(parser.getAirTemperature());
            }
        }
        return list;
    }

    public ArrayList<Double> get1902Values() {
        List<String> lines = getInputFromFile("src/test/resources/1902.txt");

        for(String line : lines) {
            parser.parse(line);
            if (parser.isValidTemperature()) {
                list.add(parser.getAirTemperature());
            }
        }
        return list;
    }

    public ArrayList<Double> get1903Values() {
        List<String> lines = getInputFromFile("src/test/resources/1903.all");

        for(String line : lines) {
            parser.parse(line);
            if (parser.isValidTemperature()) {
                list.add(parser.getAirTemperature());
            }
        }
        return list;
    }


    protected static List<String> getInputFromFile(String path) {
        List<String> lines = new LinkedList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            String line;
            while ((line = br.readLine()) != null) {
                lines.add(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return lines;
    }

    public static double calcMedian(ArrayList<Double> median) {
        // calc median
        double result = 0;
        if (median.size() % 2 == 0) {
            result = (median.get(median.size()/2) + median.get(median.size()/2 - 1))/2;
        } else
            result = median.get(median.size() / 2);

        return result / 10;
    }

    public static double calcPercentile(ArrayList<Double> median, double quantile) {
        double result = 0;

        Percentile percentile = new Percentile();
        double[] list = ArrayUtils.toPrimitive(median.toArray(new Double[0]));

        percentile.setData(list);
        result = percentile.evaluate(quantile);

        return result / 10;
    }

    public static void main(String[] args) {
        MedianCalculator m = new MedianCalculator();
        ArrayList<Double> median;

        // 1901
        median = m.get1901Values();
        Collections.sort(median);
        // calc median
        System.out.println("1901: " + calcMedian(median));
        median.clear();


        // 1902
        median = m.get1902Values();
        Collections.sort(median);
        // calc median
        System.out.println("1902: " + calcMedian(median));
        median.clear();


        // 1903
        median = m.get1903Values();
        Collections.sort(median);
        // calc median
        System.out.println("1903: " + calcMedian(median));
        median.clear();


        // quantile calc test with 1901
        median = m.get1901Values();
        Collections.sort(median);
        double quantile = 50;

        System.out.println("Quantile " + quantile + " -> median: " + calcPercentile(median, quantile));

    }

}

