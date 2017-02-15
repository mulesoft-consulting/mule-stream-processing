# Stream Processing Connector

```xml

     <mule-stream-processing:config name="MuleStreamProcessing__Configuration" doc:name="MuleStreamProcessing: Configuration"/>
      <http:listener-config name="HTTP_Listener_Configuration" host="0.0.0.0" port="8081" doc:name="HTTP Listener Configuration"/>
       <flow name="mule-stream-processing-exampleFlow-1">
          <http:listener config-ref="HTTP_Listener_Configuration"
              path="/1" doc:name="HTTP" />
          <byte-array-to-string-transformer doc:name="Byte Array to String"/>
          <mule-stream-processing:send config-ref="MuleStreamProcessing__Configuration"
              stream="test1" doc:name="MuleStreamProcessing" />
      </flow>
  
      <!-- Insert events into a stream (test2) -->
      <flow name="mule-stream-processing-exampleFlow-2">
          <http:listener config-ref="HTTP_Listener_Configuration"
              path="/2" doc:name="HTTP" />
          <byte-array-to-string-transformer doc:name="Byte Array to String"/>
          <mule-stream-processing:send config-ref="MuleStreamProcessing__Configuration"
              stream="test2" doc:name="MuleStreamProcessing" />
      </flow>
  
      <!-- Listen for events off streams test1 and test2 and emit messages as batch every 15 seconds -->
      <flow name="mule-stream-processing-exampleFlow1">
          <mule-stream-processing:listen
              config-ref="MuleStreamProcessing__Configuration" doc:name="MuleStreamProcessing (Streaming)"
              interval="15" timeUnit="SECONDS" streams="test1,test2" />
          <foreach doc:name="For Each">
              <logger message="*** Messages off Streams 1 and 2: #[payload] ***"
                  level="INFO" doc:name="Logger" />
          </foreach>
      </flow>
  
      <!-- Filter events off streams test1 and test2 with MEL and emit messages as batch every 15 seconds -->
      <flow name="cep-testFlow2">
          <mule-stream-processing:listen
              config-ref="MuleStreamProcessing__Configuration" doc:name="MuleStreamProcessing (Streaming)"
              interval="15" timeUnit="SECONDS" streams="test1,test2"
              filterExpression="return payload.state == &quot;NY&quot;" />
  
          <foreach doc:name="For Each">
              <logger message="*** STREAM 2 Got a message: #[payload] ***"
                  level="INFO" doc:name="Logger" />
          </foreach>
      </flow>
  
      <!-- Use SQL to query off stream test1 and emit messages in real time when the foo header is '1234 -->
      <flow name="cep-testFlow">
         <mule-stream-processing:query config-ref="MuleStreamProcessing__Configuration"
              query="SELECT MEL('message.inboundProperties.foo',message) from test1"
              doc:name="ComplexEventProcessing (Streaming)" streams="test1,test3" />
        
          <foreach doc:name="For Each">
              <logger message="QUERY RESULTS #[message.payload]" level="INFO"
                  doc:name="Logger" />
          </foreach>
      </flow>
```

# Mule supported versions
Examples:
Mule 3.4.x, 3.5.x
Mule 3.4.1

# [Destination service or application name] supported versions
Example:
Oracle E-Business Suite 12.1 and above.

#Service or application supported modules
Example:
Oracle CRM
Oracle Financials
or 
Salesforce API v.24
Salesforce Metadata API


# Installation 
For beta connectors you can download the source code and build it with devkit to find it available on your local repository. Then you can add it to Studio

For released connectors you can download them from the update site in Anypoint Studio. 
Open Anypoint Studio, go to Help → Install New Software and select Anypoint Connectors Update Site where you’ll find all avaliable connectors.

#Usage
For information about usage our documentation at http://github.com/mulesoft/complex-event-processing.

# Reporting Issues

We use GitHub:Issues for tracking issues with this connector. You can report new issues at this link http://github.com/mulesoft/complex-event-processing/issues.# complex-event-processing-connector
