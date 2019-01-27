public class VarianceExperiment {

    public static void main(String[] args) {
        double[] values = new double[] {1.0, 1+Math.pow(10, -13), 1+Math.pow(10, -13), 1+Math.pow(10, -13)};
        double[] values2 = new double[] {1.0, 1+Math.pow(10, -13), 1+Math.pow(10, -13), 1+Math.pow(10, -13)};
        int n = 4;

        double sum = 0.0;

        for(int i = 0; i < values.length ; i++) {
            values[i] = Math.pow(values[i], 2);
        }

        for(int i = 0; i < values.length ; i++) {
            sum = sum + values[i];
        }
        sum = sum*(1/n);

        double sum2 = 0.0;

        for(int i = 0; i < values.length ; i++) {
            sum2 = sum2 + values2[i];
        }

        sum2 = sum2*(1/n);

        sum2 = Math.pow(sum2, 2);

        System.out.println(sum - sum2);





    }

}
