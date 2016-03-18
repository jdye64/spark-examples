package com.jeremydyer;

import org.apache.nifi.remote.Transaction;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.remote.client.SiteToSiteClient;
import org.apache.nifi.remote.client.SiteToSiteClientConfig;
import org.apache.nifi.remote.exception.ProtocolException;
import org.apache.nifi.spark.NiFiDataPacket;
import org.apache.nifi.spark.NiFiReceiver;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

/**
 * Created by jdyer on 3/15/16.
 */
public class Main {

    public static void main(String[] args) throws IOException {

        SiteToSiteClient.Builder builder = new SiteToSiteClient.Builder();
        SiteToSiteClientConfig config = builder
                .url("http://10.0.1.28:8080/nifi")
                .portName("Data For Spark")
                .buildConfig();

        SiteToSiteClientConfig toNiFiConfig = builder
                .url("http://10.0.1.28:8080/nifi")
                .portName("Data From Spark")
                .buildConfig();

        SiteToSiteClient client = builder.fromConfig(toNiFiConfig).build();
        final Transaction transaction = client.createTransaction(TransferDirection.SEND);
        if (transaction == null) {
            System.err.println("Unable to create a NiFi Transaction to send data");
        }

        //Bytes would be your flowfile content and map would be your desired flowfile attributes.
        transaction.send("Hello Jeremy".getBytes(), new HashMap<String, String>());
        transaction.confirm();
        transaction.complete();

        System.out.println("Data should be written to NiFi by now");


        SparkConf sparkConf = new SparkConf().setAppName("NiFi-Spark Streaming example");
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, new Duration(10000L));
        // Create a JavaReceiverInputDStream using a NiFi receiver so that we can pull data from
        // specified Port

        JavaReceiverInputDStream packetStream = ssc.receiverStream(new NiFiReceiver(config, StorageLevel.MEMORY_ONLY()));

        JavaDStream text = packetStream.map(new Function<NiFiDataPacket, String>() {
            public String call(final NiFiDataPacket dataPacket) throws Exception {
                System.out.println("Mapping NiFi Data ...");
                return new String(dataPacket.getContent(), StandardCharsets.UTF_8);
            }
        });

//        // Map the data from NiFi to text, ignoring the attributes
//        JavaDStream text = packetStream.map(new Function() {
//            public String call(final NiFiDataPacket dataPacket) throws Exception {
//                return new String(dataPacket.getContent(), StandardCharsets.UTF_8);
//            }
//        });

        text.print();

        System.out.println("Data from NiFi ->" + text);

//        // Extract the 'uuid' attribute
//        JavaDStream text = packetStream.map(new Function() {
//            public String call(final NiFiDataPacket dataPacket) throws Exception {
//                return dataPacket.getAttributes().get("uuid");
//            }
//        });

        System.out.println("Spark Streaming successfully initialized");
        ssc.start();
        ssc.awaitTermination();
    }
}
