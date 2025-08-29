# Pass the account number as an argument
AWS_ACCOUNT_ID=$1

if [ -z "$AWS_ACCOUNT_ID" ]; then
  echo "Error: Please provide the AWS account number as the first argument."
  exit 1
fi

# Authenticate Docker to ECR
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin $AWS_ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com

# Create repo (once)
aws ecr create-repository --repository-name fraud-pipeline

# Build image
docker build --platform linux/amd64 -t fraud-pipeline .

# Tag for ECR
docker tag fraud-pipeline:latest $AWS_ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com/fraud-pipeline:latest

# Push to ECR
docker push $AWS_ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com/fraud-pipeline:latest

# Command to inspect manifest:
# docker manifest inspect $AWS_ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com/fraud-pipeline:latest