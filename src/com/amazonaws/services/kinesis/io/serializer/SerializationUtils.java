package com.amazonaws.services.kinesis.io.serializer;

import java.io.IOException;

/**
 * Helper methods for managing Kinesis Serialisation
 * 
 * @author meyersi
 */
public class SerializationUtils {
    /**
     * Ensure that the generated binary representation will conform to Kinesis
     * wire format requirements
     * 
     * @param check
     * @return
     * @throws Exception
     */
    // Kinesis Maximum Byte Length is 50KB
    public static final int maxObjectSize = 50 * 1024;

    public static byte[] safeReturnData(byte[] check) throws IOException {
        if (check.length > maxObjectSize) {
            throw new IOException(String.format(
                    "Serialised byte length exceeds maximum length of %s", maxObjectSize));
        }

        return check;
    }
}
