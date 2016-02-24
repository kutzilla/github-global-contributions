package de.fhms.mdm.github_data_processing;

import com.holdenkarau.spark.testing.SharedJavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.Serializable;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

/**
 * Created by Matthias on 20.02.16.
 */
public class CommitDataProcessorTest extends SharedJavaSparkContext implements Serializable {

    private CommitDataProcessor testCommitDataProcessor;

    @Before
    public void setUp() {
        jsc().parallelize(Arrays.asList());
        testCommitDataProcessor = new CommitDataProcessor();
        testCommitDataProcessor.setJavaSparkContext(jsc());
    }

    @Test
    @Ignore
    public void testUserProcessing() {
        String testFilePath = getClass().getClassLoader().getResource("test_commit.dat").getFile();
        JavaRDD<User> users = testCommitDataProcessor.processUserData(testFilePath);
        User expectedUser = new User();
        expectedUser.setEmail("kutz.matthias@gmail.com");
        expectedUser.setLogin("kutzilla");
        assertEquals(expectedUser, users.collect().get(0));
    }

}
