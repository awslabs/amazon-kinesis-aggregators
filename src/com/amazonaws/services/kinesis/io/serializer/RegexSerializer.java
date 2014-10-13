package com.amazonaws.services.kinesis.io.serializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.amazonaws.services.kinesis.aggregators.InputEvent;

public class RegexSerializer extends StringSerializer<RegexSerializer> implements
        IKinesisSerializer<List<List<String>>, byte[]> {
    private String regexPattern;

    private Pattern p;

    private Matcher m;

    public RegexSerializer(String regexPattern) {
        this.regexPattern = regexPattern;
        p = Pattern.compile(this.regexPattern);
    }

    public List<List<String>> toClass(InputEvent event) throws IOException {
        List<List<String>> output = new ArrayList<>();
        String[] items;
        try {
            items = super.getItems(event);

            for (String s : items) {
                List<String> elements = new ArrayList<>();

                if (m == null) {
                    m = p.matcher(s);
                } else {
                    m.reset(s);
                }
                if (m.find() && m.groupCount() > 0) {
                    for (int i = 1; i < m.groupCount() + 1; i++) {
                        elements.add(m.group(i));
                    }

                    output.add(elements);
                }
            }

            return output;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    public byte[] fromClass(List<List<String>> content) throws IOException {
        // Can't reverse engineer the original regex from a string list, so dont
        // try
        throw new IOException(new UnsupportedOperationException());
    }
}
