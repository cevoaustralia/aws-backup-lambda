#!/bin/bash

set -e

REGION="ap-southeast-2"


# Install dependancies locally, so they get included in the zip file
pip install -r requirements.txt -t lambda

aws cloudformation package \
  --template-file cloudformation.yaml   \
  --output-template-file generated-cloudformation.yaml    \
  --s3-bucket ${BUCKET}


aws cloudformation deploy \
  --template-file generated-cloudformation.yaml \
  --stack-name "aws-backup-lambda" \
  --region ${REGION} \
  --capabilities CAPABILITY_IAM
