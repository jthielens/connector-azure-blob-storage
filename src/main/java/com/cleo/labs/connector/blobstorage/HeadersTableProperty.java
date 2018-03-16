package com.cleo.labs.connector.blobstorage;

import java.util.HashMap;

import com.cleo.connector.api.annotations.Array;
import com.cleo.connector.api.annotations.Display;
import com.cleo.connector.api.annotations.Property;
import com.cleo.connector.api.interfaces.IConnectorProperty;
import com.cleo.connector.api.property.PropertyBuilder;
import com.google.common.base.Strings;
import com.google.gson.Gson;

@Array
public class HeadersTableProperty {
    private static final Gson GSON = new Gson();

    /**
     * Display value for the Headers Table property
     * @param value the Routing Table property value (a JSON array)
     * @return "n Records" (or "1 Record")
     */
    @Display
    public String display(String value) {
        int size = toHeaders(value).size();
        return String.format("%d Record%s", size, size==1?"":"s");
    }
  
    @Property
    final IConnectorProperty<Boolean> enabled = new PropertyBuilder<>("Enabled", true)
        .setRequired(true)
        .build();

    @Property
    final public IConnectorProperty<String> header = new PropertyBuilder<>("Header", "")
        .setDescription("Header name")
        .build();

    @Property
    final public IConnectorProperty<String> content = new PropertyBuilder<>("Value", "")
        .setDescription("Header value")
        .build();

    public static class HeaderValue {
        private boolean enabled;
        private String header;
        private String value;
        public HeaderValue enabled(boolean enabled) {
            this.enabled = enabled;
            return this;
        }
        public boolean enabled() {
            return enabled;
        }
        public HeaderValue header(String header) {
            this.header = header;
            return this;
        }
        public String header() {
            return header;
        }
        public HeaderValue value(String value) {
            this.value = value;
            return this;
        }
        public String value() {
            return value;
        }
        public HeaderValue(boolean enabled, String header, String value) {
            this.enabled = enabled;
            this.header = header;
            this.value = value;
        }
        public HeaderValue() {
            this(false, null, null);
        }
    }

    /**
     * Deserialize the JSON array into a Map<String,String>
     * @param value the JSON array (may be {@code null})
     * @return a (possibly empty but not null) Map<String,String>
     */
    public static HashMap<String,String> toHeaders(String value) {
        HeaderValue[] headers = Strings.isNullOrEmpty(value) ? new HeaderValue[0] : GSON.fromJson(value, HeaderValue[].class);
        HashMap<String,String> result = new HashMap<>(headers.length);
        for (HeaderValue header : headers) {
            if (header.enabled()) {
                result.put(header.header(), header.value());
            }
        }
        return result;
    }
}
