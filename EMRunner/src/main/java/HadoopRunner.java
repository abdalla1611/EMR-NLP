import com.amazonaws.auth.*;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.*;
import org.apache.log4j.BasicConfigurator;

import java.time.LocalDateTime;

public class HadoopRunner {
    public static void main(String[] args) {
        AWSCredentials credentials = new BasicSessionCredentials
                ("ASIAZKQLNCOGRQL47IX5",
                        "9SXYtDyQ4vkfLuvcws1biHsJp9hA/V/vU6R853OT",
                        "FwoGZXIvYXdzEHwaDALhh0339FAKxEkqhiLHAfpiBGxJWqQXRzHPx6MH5Kky0SkUz7qW7W0mYeY7AR2BRdPGizJ+BjinyAhceBEegF1wHwynBf1Imks5UJYULNY5JWal5QCpnZHVhaeC4q7ROUtXtBP35XTJ3n5bJ1MXIAwtzzdriTez6yXS3q6sRNiHbSx2AZyGbgn/PEEauAxu8IlgZKcN3M6xcxU/kf10/z/HZddXzgMVr/kk7kGHe7y+syzQdoZbOoTdcwJU/++wcqEuxnFmoqUN59EYFVkYflA/qJFZqqoo7+LpkAYyLfQDPd7KL7wPfZhvaqjZ3qqd+8QXR7ZjNloomWwFCe3V6OulE6JzK/YUDesRFw=="
                );
        final AmazonElasticMapReduce emr = AmazonElasticMapReduceClient.builder()
                .withRegion(Regions.US_EAST_1)
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .build();
        LocalDateTime now = LocalDateTime.now();
        HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig()
                .withJar("s3n://emrodaiar/steps-1.0-SNAPSHOT.jar") // This should be a full map reduce application.
                .withMainClass("StepsRunner")
                .withArgs("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data",
                        LocalDateTime.now().toString().replace(':', '-'));
        StepConfig stepConfig = new StepConfig()
                .withName("stepname")
                .withHadoopJarStep(hadoopJarStep)
                .withActionOnFailure("TERMINATE_JOB_FLOW");
        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(7)
                .withMasterInstanceType(InstanceType.M4Xlarge.toString())
                .withSlaveInstanceType(InstanceType.M4Xlarge.toString())
                .withHadoopVersion("2.7.2")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));
        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("google 3-gram statistics")
                .withInstances(instances)
                .withSteps(stepConfig)
                .withServiceRole("EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withLogUri("s3n://emrodaiar/logs/")
                .withReleaseLabel("emr-5.0.0");


        RunJobFlowResult runJobFlowResult = emr.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);
    }
}
