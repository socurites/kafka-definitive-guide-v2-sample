package com.socurites.kafka.sample.chap3.serializer;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.kafka.common.serialization.Serializer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class CustomerSerializerEx implements Serializer<CustomerSerializerEx.Customer> {


    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Serializer.super.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(String topic, Customer data) {
        byte[] serializedName;
        int stringSize;

        if (null == data) {
            return  null;
        } else {
            if (null != data.getCustomerName()) {
                serializedName = data.getCustomerName().getBytes(StandardCharsets.UTF_8);
                stringSize = serializedName.length;
            } else {
                serializedName = new byte[0];
                stringSize = 0;
            }

            ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + stringSize);
            buffer.putInt(data.getCustomerID());
            buffer.putInt(stringSize);
            buffer.put(serializedName);

            return buffer.array();
        }
    }

    @Override
    public void close() {
        Serializer.super.close();
    }

    @AllArgsConstructor
    @Getter
    protected static class Customer {
        private int customerID;
        private String customerName;

    }
}
