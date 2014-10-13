package com.amazonaws.services.kinesis.io.serializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import com.amazonaws.services.kinesis.aggregators.InputEvent;

public class JavaSerializationSerializer implements IKinesisSerializer<Object, byte[]>,
        Serializable {
    private static final long serialVersionUID = 2837410982374019823L;

    public Object toClass(InputEvent event) throws IOException {
        ByteArrayInputStream bis = new ByteArrayInputStream(event.getData());
        ObjectInput in = null;
        try {
            in = new ObjectInputStream(bis);
            return in.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException(e);
        } finally {
            try {
                bis.close();
            } catch (IOException ex) {
                ;
            }
            try {
                if (in != null) {
                    in.close();
                }
            } catch (IOException ex) {
            }
        }
    }

    public byte[] fromClass(Object o) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = null;
        try {
            out = new ObjectOutputStream(bos);
            out.writeObject(o);
            return SerializationUtils.safeReturnData(bos.toByteArray());
        } finally {
            try {
                if (out != null) {
                    out.close();
                }
            } catch (IOException ex) {
                // ignore close exception
            }
            try {
                bos.close();
            } catch (IOException ex) {
                // ignore close exception
            }
        }
    }
}
