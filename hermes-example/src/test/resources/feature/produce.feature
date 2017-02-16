Feature: Produce

  Background: 
    Given A Broker is started

  Scenario: Send a message
    Given A consumer is started
    When I send the message
    Then The consumer should receive the message
