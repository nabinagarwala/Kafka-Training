package com.zekelabs.kafka;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.zekelabs.kafka.constants.IKafkaConstants;
import com.zekelabs.kafka.consumer.ConsumerCreator;
import com.zekelabs.kafka.pojo.CustomObject;
import com.zekelabs.kafka.producer.ProducerCreator;

public class App {
	public static void main(String[] args) throws InterruptedException, IOException {
     //runProducer();
	 runConsumer();
	}

	static void runConsumer() throws IOException {
		Consumer<Long, CustomObject> consumer = ConsumerCreator.createConsumer();

		int noMessageToFetch = 0;
		FileWriter csvWriter = new FileWriter(IKafkaConstants.TARGET_FILE_NAME);
		BufferedWriter bfWriter = new BufferedWriter(csvWriter);
		while (true) {
			final ConsumerRecords<Long, CustomObject> consumerRecords = consumer.poll(1000);
			if (consumerRecords.count() == 0) {
				noMessageToFetch++;
				if (noMessageToFetch > IKafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT)
					break;
				else
					continue;
			}
			
			consumerRecords.forEach(record -> {
				try {
					Thread.sleep(4000);
					System.out.println("Record Key " + record.key());
					System.out.println("Record value " + record.value().getName());
					System.out.println("Record value " + record.value().getGender());
					System.out.println("Record value " + record.value().getAge());
					System.out.println("Record partition " + record.partition());
					System.out.println("Record offset " + record.offset());
				
					bfWriter.append(record.value().getName());
					bfWriter.append(IKafkaConstants.CVS_SPLIT_BY);
					bfWriter.append(record.value().getGender());
					bfWriter.append(IKafkaConstants.CVS_SPLIT_BY);
					bfWriter.append(record.value().getAge());
					bfWriter.append("\n");
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			});
			
			consumer.commitAsync();
			bfWriter.flush();
			
		}
		bfWriter.close();
		consumer.close();
		
	}

	
	static void runProducer() throws InterruptedException {
		Producer<Long, CustomObject> producer = ProducerCreator.createProducer();
		int lineCount = 0;
        FileInputStream fis;
        BufferedReader br = null;
        try {
			fis = new FileInputStream(IKafkaConstants.SOURCE_FILE_NAME);
			//Construct BufferedReader from InputStreamReader
	        br = new BufferedReader(new InputStreamReader(fis));
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
        

        String line = null;
        try {
			while ((line = br.readLine()) != null) {
				Thread.sleep(1000);
				// use comma as separator
			    String[] customer = line.split(IKafkaConstants.CVS_SPLIT_BY);
				
			    CustomObject co = new CustomObject();
			    co.setName(customer[0]);
			    co.setGender(customer[1]);
				co.setAge(customer[2]);
				lineCount++;
								
				final ProducerRecord<Long, CustomObject> record = new ProducerRecord<Long, CustomObject>(IKafkaConstants.TOPIC_NAME, co);
				
				try {
					RecordMetadata metadata = producer.send(record).get();
					System.out.println(lineCount);
					System.out.println("Record sent with key " + lineCount + " to partition " + metadata.partition()
							+ " with offset " + metadata.offset());
				} catch (ExecutionException e) {
					System.out.println("Error in sending record");
					System.out.println(e);
				} catch (InterruptedException e) {
					System.out.println("Error in sending record");
					System.out.println(e);
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
      
	}
}
