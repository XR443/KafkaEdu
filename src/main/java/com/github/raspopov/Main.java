package com.github.raspopov;

import com.github.raspopov.swing.log.JTextAreaOutputStream;
import com.github.raspopov.swing.log.LogTextArea;
import com.github.raspopov.swing.tabs.KafkaStreamsTab;
import com.github.raspopov.swing.tabs.KafkaTab;

import javax.swing.*;
import java.awt.*;
import java.io.PrintStream;

public class Main {

    public static void main(String[] args) {
        SwingUtilities.invokeLater(Main::createAndShowGUI);
    }

    private static void createAndShowGUI() {
        JFrame frame = new JFrame("Messaging Demo");
        frame.setPreferredSize(new Dimension(1280, 720));
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

        JPanel jPanel = new JPanel(new GridLayout(1, 1));
        frame.add(jPanel, BorderLayout.CENTER);

        JTextAreaOutputStream logOutputStream = new JTextAreaOutputStream();
        System.setOut(new PrintStream(logOutputStream));

        JTabbedPane tabbedPane = new JTabbedPane();
        tabbedPane.addTab("Kafka", KafkaTab.createTab(logOutputStream.register(new LogTextArea())));
        tabbedPane.addTab("Kafka Streams", KafkaStreamsTab.createTab(logOutputStream.register(new LogTextArea())));
        tabbedPane.addTab("RabbitMQ", makeTextPanel("Text RabbitMQ"));
        tabbedPane.setTabLayoutPolicy(JTabbedPane.SCROLL_TAB_LAYOUT);
        jPanel.add(tabbedPane);

        frame.pack();
        frame.setVisible(true);
    }

    protected static JComponent makeTextPanel(String text) {
        JPanel panel = new JPanel(false);
        JLabel filler = new JLabel(text);
        filler.setHorizontalAlignment(JLabel.CENTER);
        panel.setLayout(new GridLayout(1, 1));
        panel.add(filler);
        return panel;
    }
}
