package com.github.raspopov.swing.tabs;

import com.github.raspopov.service.KafkaStreamsService;

import javax.swing.*;

public class KafkaStreamsTab extends KafkaTab {

    private JTextField topicFromTextField;
    private JTextField topicToTextField;

    protected KafkaStreamsTab(KafkaStreamsService kafkaService, JTextArea logArea) {
        super(kafkaService, logArea);
        JPanel kafkaButtonsPanel = (JPanel) getComponent(0);

        {
            JPanel createStreamPanel = new JPanel();

            JButton createConsumer = new JButton("Create Stream");
            createConsumer.addActionListener(e -> kafkaService.createKafkaStream("testApplicationId",
                    topicFromTextField.getText().trim(),
                    topicToTextField.getText().trim()));
            createStreamPanel.add(createConsumer);

            JLabel topicLabel = new JLabel(" From: ");
            createStreamPanel.add(topicLabel);
            topicFromTextField = new JTextField(10);
            topicFromTextField.setHorizontalAlignment(JLabel.LEFT);
            createStreamPanel.add(topicFromTextField);

            JLabel groupLabel = new JLabel(" To: ");
            createStreamPanel.add(groupLabel);
            topicToTextField = new JTextField(10);
            topicToTextField.setHorizontalAlignment(JLabel.LEFT);
            createStreamPanel.add(topicToTextField);

            kafkaButtonsPanel.add(createStreamPanel);
        }
    }

    public static KafkaStreamsTab createTab(JTextArea log) {
        return new KafkaStreamsTab(new KafkaStreamsService(), log);
    }
}
