package UE4;

import UE2.NcdcRecordParser;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.math3.stat.descriptive.moment.Kurtosis;
import org.apache.commons.math3.stat.descriptive.moment.Skewness;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class VSKCalculator {

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

    private static List<String> getInputFromFile(String path) {
        List<String> lines = new LinkedList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            String line = new String();
            while ((line = br.readLine()) != null) {
                lines.add(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return lines;
    }

    public static double calcVariance(ArrayList<Double> values) {
        double var = 0;
        double sum = 0.0;
        double sum2 = 0.0;

        for (int i = 0; i < values.size(); i++) {
            values.set(i, Math.pow(values.get(i), 2));
        }

        for (int k = 0; k < values.size(); k++) {
            sum = sum + values.get(k);
        }

        sum = sum * (1/values.size());

        for (int j = 0; j < values.size(); j++) {
            sum2 = sum2 + values.get(j);
        }

        sum2 = sum * (1/values.size());
        sum2 = Math.pow(sum2, 2);

        var = (sum - sum2);

        return var;
    }

    public static double calcSkewness(ArrayList<Double> values) {
        Double[] list = values.toArray(new Double[values.size()]);
        double[] d = ArrayUtils.toPrimitive(list);

        Skewness s = new Skewness();

        return s.evaluate(d, 0, d.length);
    }

    public static double calcKurtosis(ArrayList<Double> values) {
        Double[] list = values.toArray(new Double[values.size()]);
        double[] d = ArrayUtils.toPrimitive(list);

        Kurtosis k = new Kurtosis();

        return k.evaluate(d, 0, d.length);
    }


    public static void main(String[] args) {
        VSKCalculator m = new VSKCalculator();
        ArrayList<Double> test;

        // 1901
        test = m.get1901Values();
        // calc median
        System.out.println("1901 Variance: " + calcVariance(test));
        System.out.println("1901 Skewness: " + calcSkewness(test));
        System.out.println("1901 Kurtosis: " + calcKurtosis(test));
        test.clear();


        // 1902
        test = m.get1902Values();
        // calc median
        System.out.println("1902 Variance: " + calcVariance(test));
        System.out.println("1902 Skewness: " + calcSkewness(test));
        System.out.println("1902 Kurtosis: " + calcKurtosis(test));
        test.clear();


        // 1903
        test = m.get1903Values();
        // calc median
        System.out.println("1903 Variance: " + calcVariance(test));
        System.out.println("1903 Skewness: " + calcSkewness(test));
        System.out.println("1903 Kurtosis: " + calcKurtosis(test));
        test.clear();



    }
}
