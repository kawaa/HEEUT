/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package pl.edu.icm.coansys.heeut;

import java.io.File;
import java.io.IOException;
import org.apache.pig.tools.parameters.ParseException;
import org.apache.pig.pigunit.PigTest;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author akawa
 */
public class TestPig {

    private PigTest test;
    private String AVGDIV_PIG = "src/test/pig/avgdiv.pig";
    private String AVGDIV_PARAMS_PIG = "src/test/pig/avgdiv_params.pig";
    private String DIVS_DAT = "src/test/resource/input/avgdiv/divs.dat";
 
    @BeforeClass
    public static void setUp() throws Exception {
    }

    @Test
    public void testTextInput() throws ParseException, IOException {
        test = new PigTest(AVGDIV_PIG);

        // Rather than read from a file, generate synthetic input.
        // Format is one record per line, tab separated.
        String[] input = {
            "NYSE\tCPO\t2009-12-30\t0.14",
            "NYSE\tCPO\t2009-01-06\t0.14",
            "NYSE\tCCS\t2009-10-28\t0.414",
            "NYSE\tCCS\t2009-01-28\t0.414",
            "NYSE\tCIF\t2009-12-09\t0.029",};

        String[] output = {"(0.22739999999999996)"};

        // Run the example script using the input we constructed
        // rather than loading whatever the load statement says.
        // "divs" is the alias to override with the input data
        // As with the previous example "avgdiv" is the alias
        // to test against the value(s) in output.
        test.assertOutput("divs", input, "avgdiv", output);
    }

    @Test
    public void testFileInput() throws ParseException, IOException {
        String[] args = {"input=" + DIVS_DAT};
        test = new PigTest(AVGDIV_PARAMS_PIG, args);
        
        String[] output = {"(0.22739999999999996)"};
        test.assertOutput("avgdiv", output);
    }
    
    @Test
    public void testFileOutput() throws ParseException, IOException {
        String[] args = {"input=" + DIVS_DAT};
        test = new PigTest(AVGDIV_PARAMS_PIG, args);
        
        test.assertOutput(new File("src/test/resource/exp/avgdiv/avgdiv.exp"));
    }
}
