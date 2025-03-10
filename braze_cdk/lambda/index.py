import boto3
import logging
from botocore.exceptions import ClientError
import fastavro
import json
import io
from urllib.parse import unquote

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
s3 = boto3.client('s3')
bedrock = boto3.client('bedrock-runtime')
table = boto3.resource('dynamodb').Table('braze_user_personalization')

def generate_personalized_text(entry):
    try:
        if not isinstance(entry, dict):
            raise ValueError("Entry must be a dictionary")

        prompt = ('You help in crearting marketing email content for video on demand streaming company. '
                  'You MUST answer in JSON format only.'
                  'DO NOT use any other format while answering the question.'
                  'Your task is to create json object contains 3 main elements: email title, heading and details (intro, episode details, cta).'  
                 'The email is to promote new episode of series: ' + 
                 json.dumps(entry["properties"])+
                 'The series details: "In a world where memories can be implanted, edited, and even stolen, a gifted memory detective must solve a series of murders linked to a powerful tech corporation."'
                 'create the json object promoting episode two with factitious story. Use data to personalize the messaging')

        body = json.dumps({
            "prompt": '\n\nHuman:' + prompt + ' \n\nAssistant:',
            "max_tokens_to_sample": 500,
            "temperature": 1,
            "top_p": 0.9,
            "top_k": 500,
        })

        modelId = 'anthropic.claude-v2'
        accept = 'application/json'
        contentType = 'application/json'

        try:
            response = bedrock.invoke_model(
                body=body,
                modelId=modelId,
                accept=accept,
                contentType=contentType
            )
        except ClientError as e:
            logger.error(f"Bedrock API error: {str(e)}")
            raise

        try:
            response_body = json.loads(response.get('body').read())
            completion = response_body.get('completion')
            
            if not completion:
                raise ValueError("Empty completion in Bedrock response")

            json_blocks = completion.split('```json')
            if len(json_blocks) < 2:
                raise ValueError("No JSON block found in completion")

            json_content = json_blocks[1].split('```')[0]
            return json.loads(json_content)

        except json.JSONDecodeError as e:
            logger.error(f"Error parsing JSON response: {str(e)}")
            raise
            
    except Exception as e:
        logger.error(f"Error in generate_personalized_text: {str(e)}")
        raise

def lambda_handler(event, context):
    try:
        # Input validation
        if not event.get('Records'):
            logger.error("No records found in event")
            raise ValueError("No records found in event")
            
        # Get the bucket name and file key from the event
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        file_key = unquote(event['Records'][0]['s3']['object']['key'])
        
        logger.info(f"Processing file {file_key} from bucket {bucket_name}")
        
        try:
            # Read the Avro file from S3
            response = s3.get_object(Bucket=bucket_name, Key=file_key)
            avro_data = response['Body'].read()
        except ClientError as e:
            logger.error(f"Failed to read file from S3: {str(e)}")
            raise
            
        try:
            # Deserialize the Avro data
            records = fastavro.reader(io.BytesIO(avro_data))
            data = [record for record in records]
        except Exception as e:
            logger.error(f"Failed to parse Avro data: {str(e)}")
            raise
            
        processed_count = 0
        error_count = 0
        
        for entry in data:
            if entry.get("name") == 'view_content':
                try:
                    logger.info(f"Processing view_content for user {entry.get('user_id')}")
                    
                    if not entry.get("user_id"):
                        logger.warning("Skipping entry - missing user_id")
                        error_count += 1
                        continue
                        
                    personalized_content = generate_personalized_text(entry)
                    
                    # Store in DynamoDB
                    table.put_item(
                        Item={
                            'user_id': entry["user_id"],
                            'data': personalized_content,
                            'timestamp': context.get_remaining_time_in_millis()
                        }
                    )
                    processed_count += 1
                    logger.info(f"Successfully processed entry for user {entry['user_id']}")
                    
                except ClientError as e:
                    logger.error(f"DynamoDB error for user {entry.get('user_id')}: {str(e)}")
                    error_count += 1
                except Exception as e:
                    logger.error(f"Error processing entry: {str(e)}")
                    error_count += 1
                    
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Processing complete',
                'processed_count': processed_count,
                'error_count': error_count,
                'file_processed': file_key
            })
        }
        
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Internal server error',
                'error': str(e)
            })
        }