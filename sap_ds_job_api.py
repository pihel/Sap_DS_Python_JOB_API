import requests
import xml.etree.ElementTree as ET
import time
import datetime

class DSJobApi:
    def __init__(self):
        #service headers
        self.logon_header   = { 'SOAPAction' : '"function=Logon"', 'Content-Type' : 'text/xml'}
        self.status_header  = { 'SOAPAction' : '"jobAdmin=Get_BatchJob_Status"', 'Content-Type' : 'text/xml'}
        self.error_header   = { 'SOAPAction' : '"jobAdmin=Get_Error_Log"', 'Content-Type' : 'text/xml'}
        self.trace_header   = { 'SOAPAction' : '"jobAdmin=Get_Trace_Log"', 'Content-Type' : 'text/xml'}
        self.logout_header  = { 'SOAPAction' : '"function=Logout"', 'Content-Type' : 'text/xml'}
        
        self.status_xml = """<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ser="http://www.businessobjects.com/DataServices/ServerX.xsd">
           <soapenv:Header>
              <ser:session>
                 <SessionID>$1</SessionID>
              </ser:session>
           </soapenv:Header>
           <soapenv:Body>
              <ser:batchJobStatusRequest>
                 <runID>$2</runID>
                 <repoName>$3</repoName>
              </ser:batchJobStatusRequest>
           </soapenv:Body>
        </soapenv:Envelope>"""
        
        self.error_xml = """<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ser="http://www.businessobjects.com/DataServices/ServerX.xsd">
           <soapenv:Header>
              <ser:session>
                 <SessionID>$1</SessionID>
              </ser:session>
           </soapenv:Header>
           <soapenv:Body>
              <ser:ErrorLogRequest>
                 <runID>$2</runID>
                 <repoName>$3</repoName>
              </ser:ErrorLogRequest>
           </soapenv:Body>
        </soapenv:Envelope>"""
        
        self.trace_xml = """<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ser="http://www.businessobjects.com/DataServices/ServerX.xsd">
           <soapenv:Header>
              <ser:session>
                 <SessionID>$1</SessionID>
              </ser:session>
           </soapenv:Header>
           <soapenv:Body>
              <ser:TraceLogRequest>
                 <runID>$2</runID>
                 <repoName>$3</repoName>
              </ser:TraceLogRequest>
           </soapenv:Body>
        </soapenv:Envelope>"""
        
        self.logout_xml = """<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ser="http://www.businessobjects.com/DataServices/ServerX.xsd">
           <soapenv:Header>
              <ser:session>
                 <SessionID>$1</SessionID>
              </ser:session>
           </soapenv:Header>
           <soapenv:Body>
              <ser:Logout_Input/>
           </soapenv:Body>
        </soapenv:Envelope>"""
        
    def logon(self, logon_xml):
        """
        Logon to Service

        :param str logon_xml: Sap ds server params
        :return: host name and session id
        :rtype: Tuple(str, str)
        """
        root = ET.fromstring(logon_xml)
        host = root.find('.//*/host').text
        logon_request = requests.post(host, headers=self.logon_header, data=logon_xml)
        logon_root = ET.fromstring(logon_request.text)
        return (host, logon_root.find('.//*/SessionID').text)
        
        
    def logout(self, host, session_id):
        """
        Logout from Service

        :param str host: Sap ds host name
        :param str session_id: session id for logout
        :return: logout status
        :rtype: str
        """
        parsed_logout_xml = self.logout_xml.replace("$1", session_id)
        logout_request = requests.post(host, headers=self.logout_header, data=parsed_logout_xml)
        logout_root = ET.fromstring(logout_request.text)
        return logout_root.find('.//*/status').text
        
    def run(self, host, session_id, job_name, job_xml):
        """
        Run job by name and xml params

        :param str host: job server host
        :param str session_id: logon session id
        :param str job_xml: job name
        :param str job_xml: job xml for run
        :return: run id and repo name
        :rtype: Tuple(str, str)
        """
        parsed_job_xml = job_xml.replace("$1", session_id)
        parsed_job_header = { 'SOAPAction' : '"job=' + job_name + '"',  'Content-Type' : 'text/xml'}
        job_request = requests.post(host, headers=parsed_job_header, data=parsed_job_xml)
        job_root = ET.fromstring(job_request.text)
        return (job_root.find('.//*/rid').text, job_root.find('.//*/repoName').text)
        
    def getStatus(self, host, session_id, run_id, repo_name):
        """
        Get Job status by run id

        :param str host: job server host
        :param str session_id: logon session id
        :param str run_id: job run id from Run
        :param str repo_name: job repo name
        :return: job status
        :rtype: str
        """
        parsed_status_xml = self.status_xml.replace("$1", session_id).replace("$2", run_id).replace("$3", repo_name)
        status_request = requests.post(host, headers=self.status_header, data=parsed_status_xml)
        status_root = ET.fromstring(status_request.text)
        return status_root.find('.//*/status').text
        
    def getError(self, host, session_id, run_id, repo_name):
        """
        Get Job error text by run id

        :param str host: job server host
        :param str session_id: logon session id
        :param str run_id: job run id from Run
        :param str repo_name: job repo name
        :return: job error text
        :rtype: str
        """
        parsed_error_xml = self.error_xml.replace("$1", session_id).replace("$2", run_id).replace("$3", repo_name)
        error_request = requests.post(host, headers=self.error_header, data=parsed_error_xml)
        error_root = ET.fromstring(error_request.text)
        return error_root.find('.//*/error').text
        
    def getTrace(self, host, session_id, run_id, repo_name):
        """
        Get Job trace text by run id

        :param str host: job server host
        :param str session_id: logon session id
        :param str run_id: job run id from Run
        :param str repo_name: job repo name
        :return: job trace text
        :rtype: str
        """
        parsed_trace_xml = self.trace_xml.replace("$1", session_id).replace("$2", run_id).replace("$3", repo_name)
        trace_request = requests.post(host, headers=self.trace_header, data=parsed_trace_xml)
        trace_root = ET.fromstring(trace_request.text)
        return trace_root.find('.//*/trace').text


class DSJob(DSJobApi):
    ''' 
    #example usage
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
    '''

    #host and input params
    logon_xml = None
    job_name = None
    #job xml parameters
    job_xml = None
    
    #job host adress
    host = None
    #session key
    session_id = None
    #job process key
    run_id = None
    #job repo name
    repo_name = None
    
    #job poll freq
    step_seconds   = 60
    #job poll timeout
    timeout_seconds = 3600
    
    def __init__(self, logon_xml, job_name, job_xml):
        super().__init__()
        
        self.logon_xml = logon_xml
        self.job_name = job_name
        self.job_xml = job_xml
        
        (self.host, self.session_id) = super().logon(logon_xml)
        
    def __str__(self):
        return 'Host: {0}\nJob: {1}, Repo: {2}, Run: {3}'.format(self.host, self.job_name, self.repo_name, self.run_id)
    
    def run(self):
        (self.run_id, self.repo_name) = super().run(self.host, self.session_id, self.job_name, self.job_xml)
        
    def getStatus(self):
        if not self.run_id:
            raise Exception('Plese execute before [[run]]')
        return super().getStatus(self.host, self.session_id, self.run_id, self.repo_name)
        
    def runWait(self, timeout_seconds = 3600, debug_msg = True):
        self.timeout_seconds = timeout_seconds
        
        #get final job status
        submit_start = datetime.datetime.now()
        submit_time = 0
        
        self.run()
        
        if debug_msg:
            print(self)
        
        status = self.getStatus()
        
        while(status == "running"):
            #job status
            try:
                status = self.getStatus()
            except Exception as E:
                print("Error getStatus: " + E.__str__() + ". Retry...")
            
            #inform about status and sleep
            if debug_msg:
                print(str(submit_time) + "s. " + status )
            time.sleep(self.step_seconds)
            
            #check timeout
            current_datetime = datetime.datetime.now()
            submit_time = (current_datetime - submit_start).seconds
            if submit_time > self.timeout_seconds:
                raise Exception('Timeout ' + str(self.timeout_seconds) + 's. has occurred')
        
        #return error
        if status == "running":
            raise Exception('Timeout ' + str(self.timeout_seconds) + 's. has occurred')
        elif status == "error":
            raise Exception(super().getError(self.host, self.session_id, self.run_id, self.repo_name))
        
        #or trace
        return super().getTrace(self.host, self.session_id, self.run_id, self.repo_name)
    
    def __del__(self):
        super().logout(self.host, self.session_id)


