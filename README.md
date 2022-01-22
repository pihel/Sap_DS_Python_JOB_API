Usage: 
```python
dj = DSJob("""<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ser="http://www.businessobjects.com/DataServices/ServerX.xsd">
    <soapenv:Header>
      <host>http://10.2.110.73:8080/DataServices/servlet/webservices?ver=2.1</host>
   </soapenv:Header>
   <soapenv:Body>
      <ser:LogonRequest>
         <username>hadoop</username>
         <password>***</password>
         <cms_system>of-sapdsdw-02</cms_system>
         <cms_authentication>secEnterprise</cms_authentication>
      </ser:LogonRequest>
   </soapenv:Body>
</soapenv:Envelope>""", "JB_API", """<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ser="http://www.businessobjects.com/DataServices/ServerX.xsd">
   <soapenv:Header>
      <ser:session>
         <SessionID>$1</SessionID>
      </ser:session>
   </soapenv:Header>
   <soapenv:Body>
      <ser:JB_API_Job>
         <job_parameters>
            <job_system_profile>dev</job_system_profile>
            <job_server>JobServer_Win</job_server>
            <trace>Session</trace>
            <trace>Workflow</trace>
            <trace>Dataflow</trace>
         </job_parameters>
      </ser:JB_API_Job>
   </soapenv:Body>
</soapenv:Envelope>""")

dj.runWait()

del dj 
```


More info: http://blog.skahin.ru/
