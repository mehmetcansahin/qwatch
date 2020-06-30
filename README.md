# QWATCH
SQS Queue Watch

According to the information you provided in the configuration, it listens on sqs queue and runs the program you want according to the incoming data.

config.json
```json
{
  "aws_access_key_id": "",
  "aws_secret_access_key": "",
  "queue_url": "",
  "failed_queue_url": "",
  "region": "eu-central-1",
  "commands": [
    {
      "name": "program_name",
      "program": "program_path",
      "args": ["arguments"]
    }
  ]
}
```