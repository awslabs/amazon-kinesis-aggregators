package com.amazonaws.services.kinesis.io.serializer;

import java.nio.charset.Charset;

import com.amazonaws.services.kinesis.aggregators.InputEvent;

public abstract class StringSerializer<T extends StringSerializer<T>> {
    protected String charset = "UTF-8";

    protected String itemTerminator = "\n";

    /**
     * Builder method to apply a non-default character set to text based
     * serialisation operations (default UTF-8)
     * 
     * @param charset
     * @return
     */
    @SuppressWarnings("unchecked")
    public T withCharset(String charset) {
        // test that this is a valid character set
        Charset test = Charset.forName(charset);

        // use it
        this.charset = charset;

        return (T) this;
    }

    /**
     * Build method to apply a non-default item terminator (default \n)
     * 
     * @param itemTerminator
     * @return
     */
    @SuppressWarnings("unchecked")
    public T withItemTerminator(String terminator) {
        this.itemTerminator = terminator;
        return (T) this;
    }

    protected String[] getItems(InputEvent event) throws Exception {
        // convert the content to a string in the supplied character set
        String content = new String(event.getData(), this.charset);

        // break into items using line terminator
        return content.split(this.itemTerminator);
    }
}
