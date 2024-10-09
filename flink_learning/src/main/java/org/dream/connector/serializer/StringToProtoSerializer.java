package org.dream.connector.serializer;

import autovalue.shaded.kotlinx.metadata.internal.protobuf.ByteString;
import com.google.cloud.flink.bigquery.sink.exceptions.BigQuerySerializationException;
import com.google.cloud.flink.bigquery.sink.serializer.BigQueryProtoSerializer;

public class StringToProtoSerializer extends BigQueryProtoSerializer {
    @Override
    public com.google.protobuf.ByteString serialize(Object record) throws BigQuerySerializationException {
        if (record == null) {
            throw new BigQuerySerializationException("Record cannot be null");
        }
        return com.google.protobuf.ByteString.copyFromUtf8((String) record);
    }
}
