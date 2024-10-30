Deployment Instructions
This guide provides step-by-step instructions for deploying resources using AWS CDK.
 
Prerequisites
Before starting, ensure you have the following prerequisites installed and configured:
 
AWS CDK installed locally
 
Deployment Steps
List Stacks:
 
cdk ls
 
Deploy App:
 
Deploy your first application using the following command:
 
cdk deploy
 
Update Parameters:
 
Navigate to the parameters.py file and set the following parameters according to your requirement:
 
 
Redeploy App 1:
 
After updating the parameters, redeploy the stack:
 
cdk deploy
 
Additional Notes:
Ensure you have the necessary permissions and access configured in AWS for deploy