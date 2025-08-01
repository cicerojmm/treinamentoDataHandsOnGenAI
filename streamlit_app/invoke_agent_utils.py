import json
import os
import base64
from boto3.session import Session
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from requests import request

class BedrockAgent:
    def __init__(self, agent_id="2ATYVI53EX", agent_alias_id="PBUBUBXNW3", region="us-east-2"):
        self.agent_id = agent_id
        self.agent_alias_id = agent_alias_id
        self.region = region
        os.environ["AWS_REGION"] = region

    def _sigv4_request(self, url, method='POST', body=None, headers=None, service='bedrock'):
        """Send HTTP request signed with SigV4"""
        credentials = Session().get_credentials().get_frozen_credentials()
        req = AWSRequest(method=method, url=url, data=body, headers=headers)
        SigV4Auth(credentials, service, self.region).add_auth(req)
        req = req.prepare()
        return request(method=req.method, url=req.url, headers=req.headers, data=req.body)

    def ask_question(self, question, session_id, end_session=False):
        """Send question to Bedrock agent"""
        url = f'https://bedrock-agent-runtime.{self.region}.amazonaws.com/agents/{self.agent_id}/agentAliases/{self.agent_alias_id}/sessions/{session_id}/text'
        
        payload = {
            "inputText": question,
            "enableTrace": True,
            "endSession": end_session
        }
        
        response = self._sigv4_request(
            url=url,
            body=json.dumps(payload),
            headers={'content-type': 'application/json', 'accept': 'application/json'}
        )
        
        return self._decode_response(response)

    def _decode_response(self, response):
        """Decode Bedrock agent response"""
        content = ""
        for line in response.iter_content():
            try:
                content += line.decode('utf-8')
            except Exception as e:
                print(f"Ocorreu um erro inesperado: {type(e).__name__} - {e}")
                print(f"Pedaco de dado: {e}")
                continue

        split_response = content.split(":message-type")
        
        # Extract final response
        last_response = split_response[-1]
        if "bytes" in last_response:
            encoded_response = last_response.split('"')[3]
            decoded = base64.b64decode(encoded_response)
            final_response = decoded.decode('utf-8')
        else:
            try:
                part1 = content[content.find('finalResponse')+len('finalResponse":'):]
                part2 = part1[:part1.find('"}')+2]
                final_response = json.loads(part2)['text']
            except:
                final_response = "Error processing response"

        # Clean response
        final_response = final_response.replace('"', "").replace("{input:{value:", "").replace(",source:null}}", "")
        
        return content, final_response

# Global instance
agent = BedrockAgent()

def lambda_handler(event, context):
    """Lambda handler for Bedrock agent requests"""
    try:
        session_id = event["sessionId"]
        question = event["question"]
        end_session = event.get("endSession", False)
        
        if isinstance(end_session, str):
            end_session = end_session.lower() == "true"
        
        response, trace_data = agent.ask_question(question, session_id, end_session)
        
        return {
            "status_code": 200,
            "body": json.dumps({"response": response, "trace_data": trace_data})
        }
    except Exception as e:
        return {
            "status_code": 500,
            "body": json.dumps({"error": str(e)})
        }