package org.mule.modules.complexeventprocessing;

import java.util.HashMap;
import java.util.Map;

import org.mule.DefaultMuleMessage;
import org.mule.api.MuleMessage;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.MapSerializer;

public class MuleMessageSerializer extends Serializer<MuleMessage>  {
	
	MapSerializer serializer = new MapSerializer();

	Kryo kryo = new Kryo();

	@Override
	public MuleMessage read(Kryo kryo, Input input, Class<MuleMessage> arg2) {
		
		
		Map map  = kryo.readObject(input, HashMap.class);
		MuleMessage message = new DefaultMuleMessage(map.get("payload"), 
				ComplexEventProcessingConnector.muleContext);
		return message;
	}

	@Override
	public void write(Kryo kryo, Output output, MuleMessage message) {
				 
		
		Map<String,Object> map = new HashMap<>();
		map.put("payload", message.getPayload());
	
		Map<String,Object> inboundProperties = new HashMap<>();
		for (String name : message.getInboundPropertyNames()) {
			inboundProperties.put(name,  message.getInboundProperty(name));
		}
		
		Map<String,Object> outboundProperties = new HashMap<>();
		for (String name : message.getOutboundPropertyNames()) {
			outboundProperties.put(name,  message.getOutboundProperty(name));
		}
		
		Map<String,Object> attachments = new HashMap<>();
		for (String name : message.getAttachmentNames()) {
			attachments.put(name,  message.getAttachment(name));
		}
		
		map.put("inbound", inboundProperties);
		map.put("outbound", outboundProperties);
		map.put("attachments", attachments);
		
		kryo.writeObject(output, map);
//		serializer.write(kryo, output, map);
		

	}

}
