package UE3;

import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TemperaturePair implements Writable, WritableComparable<TemperaturePair> {

    private Text year = new Text();
    private DoubleWritable temperature = new DoubleWritable();


    public TemperaturePair() {
    }

    public TemperaturePair(String ym, int temp) {
        year.set(ym);
        temperature.set(temp);
    }

    public static TemperaturePair read(DataInput in) throws IOException {
        TemperaturePair temperaturePair = new TemperaturePair();
        temperaturePair.readFields(in);
        return temperaturePair;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        year.write(out);
        temperature.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        year.readFields(in);
        temperature.readFields(in);
    }

    @Override
    public int compareTo(TemperaturePair temperaturePair) {
        int compareValue = this.year.compareTo(temperaturePair.getYearMonth());

        return compareValue;
    }

    public Text getYearMonth() {
        return year;
    }

    public DoubleWritable getTemperature() {
        return temperature;
    }

    public Double getDoubleTemp() {
        return temperature.get();
    }

    public void setYear(String yearMonthStr) {
        year.set(yearMonthStr);
    }

    public void setTemperature(double temp) {
        temperature.set(temp);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TemperaturePair that = (TemperaturePair) o;

        if (temperature != null ? !temperature.equals(that.temperature) : that.temperature != null) return false;
        if (year != null ? !year.equals(that.year) : that.year != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = year != null ? year.hashCode() : 0;
        result = 31 * result + (temperature != null ? temperature.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "TemperaturePair{" +
                "year=" + year +
                ", temperature=" + temperature +
                '}';
    }
}