package org.mule.modules.complexeventprocessing;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import javax.activation.DataHandler;

import org.mule.DefaultMuleMessage;
import org.mule.api.MuleMessage;
import org.mule.api.serialization.SerializationException;
import org.mule.util.store.DeserializationPostInitialisable;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.FieldSerializer;

public class MuleMessageSerializer extends Serializer<MuleMessage> {

	@Override
	public MuleMessage read(Kryo kryo, Input input, Class<MuleMessage> clazz) {
		FieldSerializer<DefaultMuleMessage> serializer = new FieldSerializer<>(kryo, DefaultMuleMessage.class);

		final DefaultMuleMessage message = serializer.read(kryo, input, DefaultMuleMessage.class);
		try {
			DeserializationPostInitialisable.Implementation.init(message, ComplexEventProcessingConnector.muleContext);
		} catch (Exception e) {
			throw new SerializationException("Could not read message", e);
		}
		message.setPayload(kryo.readClassAndObject(input));

		new AttachmentDeserealizer() {

			@Override
			protected void acceptAttachment(String key, DataHandler data) throws Exception {
				message.addInboundAttachment(key, data);
			}
		}.deserealizeAttachments(kryo, input);

		new AttachmentDeserealizer() {

			@Override
			protected void acceptAttachment(String key, DataHandler data) throws Exception {
				message.addOutboundAttachment(key, data);
			}
		}.deserealizeAttachments(kryo, input);

		return message;
	}

	@Override
	public void write(Kryo kryo, Output output, final MuleMessage message) {

		FieldSerializer<DefaultMuleMessage> serializer = new FieldSerializer<>(kryo, DefaultMuleMessage.class);
		serializer.write(kryo, output, (DefaultMuleMessage) message);
		kryo.writeClassAndObject(output, message.getPayload());

		new AttachmentSerializer() {

			@Override
			protected Object getAttachment(String name) {
				return message.getInboundAttachment(name);
			}

			@Override
			protected Collection<String> getAttachmentNames() {
				return message.getInboundAttachmentNames();
			}
		}.serializeAttachments(kryo, output);

		new AttachmentSerializer() {

			@Override
			protected Object getAttachment(String name) {
				return message.getOutboundAttachment(name);
			}

			@Override
			protected Collection<String> getAttachmentNames() {
				return message.getOutboundAttachmentNames();
			}
		}.serializeAttachments(kryo, output);

	}

	private abstract class AttachmentDeserealizer {

		@SuppressWarnings("unchecked")
		protected void deserealizeAttachments(Kryo kryo, Input input) {
			Map<String, DataHandler> attachments = kryo.readObject(input, HashMap.class);

			try {
				for (Map.Entry<String, DataHandler> entry : attachments.entrySet()) {
					acceptAttachment(entry.getKey(), entry.getValue());
				}
			} catch (Exception e) {
				throw new SerializationException("Exception was found adding attachment to a MuleMessage", e);
			}
		}

		protected abstract void acceptAttachment(String key, DataHandler data) throws Exception;
	}

	private abstract class AttachmentSerializer {

		protected void serializeAttachments(Kryo kryo, Output output) {
			Map<String, DataHandler> attachments = new HashMap<String, DataHandler>();
			for (String name : getAttachmentNames()) {
				attachments.put(name, (DataHandler) getAttachment(name));
			}

			kryo.writeObject(output, attachments);

		}

		protected abstract Collection<String> getAttachmentNames();

		protected abstract Object getAttachment(String name);
	}
}
