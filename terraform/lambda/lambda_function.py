import json
from urllib import response
from urllib.parse import unquote_plus
import boto3
import os
import logging
print('Loading function')
logger = logging.getLogger()
logger.setLevel("INFO")
s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
rekognition = boto3.client('rekognition')

table = dynamodb.Table(os.getenv("DYNAMO_TABLE")) 

def lambda_handler(event, context):
    logger.info(json.dumps(event, indent=2))
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = unquote_plus(event['Records'][0]['s3']['object']['key']) # <- A modifier !!!!


    # Récupération de l'utilisateur et de l'UUID de la tâche
    try:
        user, task_uuid, _ = key.split('/')[:2]
    except ValueError:
        logger.error("Clé invalide: doit suivre format user/task_uuid/filename.jpg")
        return

    # Ajout des tags user et task_uuid
    s3.put_object_tagging(
    Bucket=bucket,
    Key=key,
    Tagging={
        'TagSet': [
            {'Key': 'user', 'Value': user},
            {'Key': 'task_uuid', 'Value': task_uuid}
        ]
    }
)
    


    # Appel à reckognition
    label_data = rekognition.detect_labels(
        Image={
            'S3Object': {
                'Bucket': bucket,
                'Name': key
            }
        },
        MaxLabels=5,
        MinConfidence=0.75

    )
    logger.info(f"Labels data : {label_data}")

    # Récupération des résultats des labels
    labels = [label['Name'] for label in label_data['Labels']]



    # Mise à jour de la table dynamodb
    try:
        table.update_item(
            Key={'id': task_uuid},
            UpdateExpression="SET rekognition_labels = :labels, user = :user",
            ExpressionAttributeValues={
                ':labels': labels,
                ':user': user
            }
        )
        logger.info(f"Item {task_uuid} updated in DynamoDB.")
    except Exception as e:
        logger.error(f"Erreur lors de la mise à jour DynamoDB: {e}")


