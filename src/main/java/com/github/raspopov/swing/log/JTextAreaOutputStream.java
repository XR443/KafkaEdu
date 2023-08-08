package com.github.raspopov.swing.log;

import javax.swing.*;
import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedList;
import java.util.List;

public class JTextAreaOutputStream extends OutputStream {
    private final List<JTextArea> areas = new LinkedList<>();

    public JTextArea register(JTextArea jTextArea) {
        areas.add(jTextArea);
        return jTextArea;
    }

    @Override
    public void write(byte[] buffer, int offset, int length) {
        final String text = new String(buffer, offset, length);
        SwingUtilities.invokeLater(() ->
        {
            for (JTextArea area : areas) {
                area.append(text);
            }
        });
    }

    @Override
    public void write(int b) throws IOException {
        write(new byte[]{(byte) b}, 0, 1);
    }

}
