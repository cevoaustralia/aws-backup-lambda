#!/bin/bash

set -e

if [ -z "$REGION" ]; then
    echo "Please define the REGION variable before executing this script."
    exit -1
fi

if [ -z "$BUCKET" ]; then
    echo "Please define the BUCKET variable before executing this script."
    exit -1
fi
if [ -z "$PROFILE" ]; then
    echo "Please define the PROFILE variable before executing this script."
    exit -1
fi

# Install dependancies locally, so they get included in the zip file
pip install -r requirements.txt -t lambda

aws cloudformation package --profile ${PROFILE}\
  --template-file cloudformation.yaml   \
  --output-template-file generated-cloudformation.yaml    \
  --s3-bucket ${BUCKET}


aws cloudformation deploy --profile ${PROFILE}\
  --template-file generated-cloudformation.yaml \
  --stack-name "aws-backup-lambda" \
  --region ${REGION} \
  --capabilities CAPABILITY_IAM \
  --parameter-overrides SuccessSNSTopicOption= ErrorSNSTopicOption=CreateSNS
