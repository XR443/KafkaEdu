package com.github.raspopov.swing.tabs;

import com.github.raspopov.service.KafkaService;

import javax.swing.*;
import java.awt.*;

public class KafkaTab extends JPanel {

    private JTextField topicConsumerTextField;
    private JTextField groupTextField;

    private JTextField offsetTextFields;
    private JCheckBox commitCheckBox;

    private JTextField topicProducerTextField;
    private JTextField dataTextField;
    private JTextField countTextField;

    KafkaTab(KafkaService kafkaService, JTextArea logArea) {
        super(false);

        setLayout(new GridLayout(1, 2));

        JPanel kafkaButtonsPanel = new JPanel();
        add(kafkaButtonsPanel);

        {
            JPanel createConsumerPanel = new JPanel();

            JButton createConsumer = new JButton("Create Consumer");
            createConsumer.addActionListener(e -> {
                if (groupTextField.getText().isEmpty()) {
                    kafkaService.createKafkaConsumer();
                } else {
                    kafkaService.createKafkaConsumer(groupTextField.getText().trim());
                }
                if (kafkaService.isConsumerCreated())
                    kafkaService.subscribeKafkaConsumer(topicConsumerTextField.getText().trim());
            });
            createConsumerPanel.add(createConsumer);

            JLabel groupLabel = new JLabel(" Group.Id: ");
            createConsumerPanel.add(groupLabel);
            groupTextField = new JTextField(10);
            groupTextField.setHorizontalAlignment(JLabel.LEFT);
            createConsumerPanel.add(groupTextField);

            JLabel topicLabel = new JLabel(" Topic: ");
            createConsumerPanel.add(topicLabel);
            topicConsumerTextField = new JTextField(10);
            topicConsumerTextField.setHorizontalAlignment(JLabel.LEFT);
            createConsumerPanel.add(topicConsumerTextField);

            kafkaButtonsPanel.add(createConsumerPanel);
        }

        {
            JPanel consumePanel = new JPanel();

            JButton button = new JButton("Consume records");
            button.addActionListener(e -> {
                String offsetStr = offsetTextFields.getText();
                long offset = offsetStr.isEmpty() ? 0 : Long.parseLong(offsetStr.trim());
                kafkaService.consumeRecords(commitCheckBox.isSelected(), offset);
            });
            consumePanel.add(button);

            JLabel offsetLabel = new JLabel(" Offset: ");
            consumePanel.add(offsetLabel);
            offsetTextFields = new JTextField(5);
            offsetTextFields.setHorizontalAlignment(JLabel.LEFT);
            consumePanel.add(offsetTextFields);

            commitCheckBox = new JCheckBox("Commit read");
            commitCheckBox.setHorizontalAlignment(JLabel.LEFT);
            consumePanel.add(commitCheckBox);

            kafkaButtonsPanel.add(consumePanel);
        }

        {
            JPanel producerPanel = new JPanel();

            JButton createConsumer = new JButton("Produce records");
            createConsumer.addActionListener(e -> {
                if (!kafkaService.isProducerCreated()) {
                    kafkaService.createKafkaProducer();
                }

                String countStr = countTextField.getText().trim();
                int count = countStr.isEmpty() ? 1 : Math.abs(Integer.parseInt(countStr));

                kafkaService.sendData(topicProducerTextField.getText().trim(),
                        dataTextField.getText().trim(),
                        count);
            });
            producerPanel.add(createConsumer);

            JLabel topicLabel = new JLabel(" Topic: ");
            producerPanel.add(topicLabel);
            topicProducerTextField = new JTextField(10);
            topicProducerTextField.setHorizontalAlignment(JLabel.LEFT);
            producerPanel.add(topicProducerTextField);

            JLabel groupLabel = new JLabel(" Data: ");
            producerPanel.add(groupLabel);
            dataTextField = new JTextField(10);
            dataTextField.setHorizontalAlignment(JLabel.LEFT);
            producerPanel.add(dataTextField);

            JLabel countLabel = new JLabel(" Count: ");
            producerPanel.add(countLabel);
            countTextField = new JTextField(5);
            countTextField.setHorizontalAlignment(JLabel.LEFT);
            producerPanel.add(countTextField);

            kafkaButtonsPanel.add(producerPanel);
        }

        {
            JScrollPane scrollPane = new JScrollPane(logArea);
            add(scrollPane);
        }
    }


    public static KafkaTab createTab(JTextArea log) {
        return new KafkaTab(new KafkaService(), log);
    }
}
