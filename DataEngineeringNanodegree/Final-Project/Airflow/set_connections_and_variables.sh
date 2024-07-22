#!/bin/bash

airflow connections add aws_credentials --conn-uri 'aws://AKIASWWJXDL36IQFUFPI:suaMr7O4PUQ7TdFcv%2Bro4%2FxA26%2FWDVMvqyAudJ6T@'

airflow connections add redshift --conn-uri "redshift://awsuser:R3dsh1ft@default-workgroup.186180377335.us-east-1.redshift-serverless.amazonaws.com:5439/dev"

airflow variables set s3_bucket sparkify-lake-house

airflow variables set s3_prefix data-pipelines