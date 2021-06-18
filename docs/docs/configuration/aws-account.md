## AWS Account Setup



Conductor supports making calls to AWS resources (e.g. htrough the AWS Lambda system task) in two ways.

1. Set the AWS account to be used when by default by updating the environment varibles with the credntials to be used when making calls. To do that, you need to have the below three envoronment variables configured. More details on these varirables and how to set them are available in the [AWS environment variables documentation page](https://docs.aws.amazon.com/sdkref/latest/guide/environment-variables.html). 
```shell
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
AWS_DEFAULT_REGION
```


2. Assume a different AWS IAM role when making a call to an AWS service. Today, this is supported only for the AWS Lambda task. First step here is to [create the IAM role]((https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_create_for-user.html)) in the other account where you will be calling to and setting two [policies](https://docs.aws.amazon.com/IAM/latest/UserGuide/access_policies_create-console.html) for the role. (1) Which account can assume this role, and (2) what can someone who has assumed this role do. Once these are done, you provice the full ARN of the role in the con figuration of the task that suppoorts this, and it will assume the role before calling AWS.
