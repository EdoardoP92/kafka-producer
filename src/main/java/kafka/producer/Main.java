package kafka.producer;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.logging.Logger;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import com.fasterxml.jackson.databind.ObjectMapper;

public class Main {
	
	public static void main(String[] args) throws Exception{
		
		/**
		 * 
		 * args[0] = bootstrap.servers
		 * args[1] = topic
		 * args[2] = numero messaggi
		 * 
		 **/
		
		Logger log = Logger.getLogger(Main.class.getSimpleName());
		
		Properties properties = new Properties();
		
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, args[0]);
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
		
		/**
		 * non inviare info riguardanti il tipo serializzato nell'header
		 * insieme al json onde evitare problemi con trusted package 
		 * o class cast exception quando il json verrà consumato
		 */
		properties.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);
		
		KafkaProducer<String,Object> producer = new KafkaProducer<>(properties);
		
		String topic = args[1];
		
		ObjectMapper om = new ObjectMapper();
		
		Object objectValue;
		try {
			objectValue = om.readValue(new File("message.json"), Object.class);
		} catch (IOException e) {
			objectValue = om.readValue(new File("..\\message.json"), Object.class);
		}
		
		Integer messages = Integer.valueOf(args[2]);
		
		int i = 0;
		
		log.info("********** START PRODUCER **********");
		while(i<messages) {
			ProducerRecord<String,Object> record = new ProducerRecord<>(topic, String.valueOf(i), objectValue);
			
			producer.send(record);
			i++;
		}
		log.info("********** END PRODUCER **********");
		producer.close();
	}
}