package com.zekelabs.kafka.constants;

public interface IKafkaConstants {
	public static String KAFKA_BROKERS = "localhost:9092";
	
	public static Integer MESSAGE_COUNT=100;
	
	public static String CLIENT_ID="client1";
	
	public static String TOPIC_NAME="csv1";
	
	public static String GROUP_ID_CONFIG="consumerGroup10";
	
	public static Integer MAX_NO_MESSAGE_FOUND_COUNT=100;
	
	public static String OFFSET_RESET_LATEST="latest";
	
	public static String OFFSET_RESET_EARLIER="earliest";
	
	public static Integer MAX_POLL_RECORDS=1;
	public static String SOURCE_FILE_NAME="C:/kafka_2.13-2.5.0/Test/SampleData.csv";
	public static String CVS_SPLIT_BY=",";
	public static String TARGET_FILE_NAME="C:/kafka_2.13-2.5.0/Test/Output.csv";
}
