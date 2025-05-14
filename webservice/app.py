#################################################################################################
##                                                                                             ##
##                                 NE PAS TOUCHER CETTE PARTIE                                 ##
##                                                                                             ##
## 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 ##
import boto3
from botocore.config import Config
import os
import uuid
from dotenv import load_dotenv
from typing import Union
import logging
from fastapi import FastAPI, Request, status, Header
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn

from getSignedUrl import getSignedUrl

load_dotenv()

app = FastAPI()
logger = logging.getLogger("uvicorn")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
	exc_str = f'{exc}'.replace('\n', ' ').replace('   ', ' ')
	logger.error(f"{request}: {exc_str}")
	content = {'status_code': 10422, 'message': exc_str, 'data': None}
	return JSONResponse(content=content, status_code=status.HTTP_422_UNPROCESSABLE_ENTITY)


class Post(BaseModel):
    title: str
    body: str

my_config = Config(
    region_name='us-east-1',
    signature_version='v4',
)

dynamodb = boto3.resource('dynamodb', config=my_config)
table = dynamodb.Table(os.getenv("DYNAMO_TABLE"))
s3_client = boto3.client('s3', config=boto3.session.Config(signature_version='s3v4'))
bucket = os.getenv("BUCKET")

## ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ##
##                                                                                                ##
####################################################################################################

from boto3.dynamodb.conditions import Key



@app.post("/posts")
async def post_a_post(post: Post, authorization: str | None = Header(default=None)):
    """
    Poste un post ! Les informations du poste sont dans post.title, post.body et le user dans authorization
    """
    logger.info(f"title : {post.title}")
    logger.info(f"body : {post.body}")
    logger.info(f"user : {authorization}")

    if not authorization:
        return JSONResponse(status_code=401, content={"message": "Authorization header missing"})

    post_id = f"POST#{str(uuid.uuid4())}"
    user_key = f"USER#{authorization}"

    item = {
        "user": user_key,
        "id": post_id,
        "title": post.title,
        "body": post.body
    }

    res = table.put_item(Item=item)


    # Doit retourner le résultat de la requête la table dynamodb
    return res


@app.get("/posts")
async def get_all_posts(user: Union[str, None] = None):
    """
    Récupère tout les postes. 
    - Si un user est présent dans le requête, récupère uniquement les siens
    - Si aucun user n'est présent, récupère TOUS les postes de la table !!
    """
    if user:
        logger.info(f"Récupération des postes de : {user}")
        res = table.query(
            KeyConditionExpression=boto3.dynamodb.conditions.Key("user").eq(user) &
                                   boto3.dynamodb.conditions.Key("id").begins_with("POST#")
        )
        items = res.get("Items", [])
      
        for item in items:
            logger.info(item.get("key"))
    else:
        logger.info("Récupération de tous les postes")
        res = table.scan()
        items = res.get("Items", [])
        items = [item for item in items if item.get("id", "").startswith("POST#")]
        

    # Construction de la réponse attendue
    posts = []
    for item in items:
        try:
            # Générer l'URL pré-signée si 'key' est présent
            object_key = item.get("key")
            if object_key:
                url = s3_client.generate_presigned_url(
                    ClientMethod='get_object',
                    Params={
                        "Bucket": bucket,
                        "Key": object_key
                    },
                    ExpiresIn=3600 
                )
            else:
                url = None

            posts.append({
                "id" : item.get("id"),
                "user": item.get("user"),
                "title": item.get("title"),
                "body": item.get("body"),
                "image": url,
                "label": item.get("labels", [])
            })
            

        except Exception as e:
            logger.warning(f"Erreur lors du traitement d’un post : {e}")

    return posts


@app.delete("/posts/{post_id}")
async def delete_post(post_id: str, authorization: str | None = Header(default=None)):
    # Doit retourner le résultat de la requête la table dynamodb
    logger.info(f"post id : {post_id}")
    logger.info(f"user: {authorization}")
    # Récupération des infos du poste
    res = table.scan(
        FilterExpression="id = :post_id_val",
        ExpressionAttributeValues={":post_id_val": f"POST#{post_id}"}
    )

    items = res.get("Items", [])
    if not items:
        return JSONResponse(status_code=404, content={"message": "Post non trouvé"})

    item = items[0]

    # Vérification de l'auteur
    if item["user"] != f"USER#{authorization}":
        return JSONResponse(status_code=403, content={"message": "Non autorisé à supprimer ce post"})

    # S'il y a une image on la supprime de S3
    if "image" in item:
        key = item["image"].split("/")[-1]
        try:
            s3_client.delete_object(Bucket=bucket, Key=key)
            logger.info(f"Image supprimée : {key}")
        except Exception as e:
            logger.warning(f"Erreur lors de la suppression de l'image : {e}")

    # Suppression de la ligne dans la base dynamodb
    delete_response = table.delete_item(
        Key={
            "user": item["user"],
            "createdAt": item["createdAt"]
        }
    )
    # Retourne le résultat de la requête de suppression
    return item

#################################################################################################
##                                                                                             ##
##                                 NE PAS TOUCHER CETTE PARTIE                                 ##
##                                                                                             ##
## 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 ##
@app.get("/signedUrlPut")
async def get_signed_url_put(filename: str,filetype: str, postId: str,authorization: str | None = Header(default=None)):
    return getSignedUrl(filename, filetype, postId, authorization)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080, log_level="debug")

## ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ##
##                                                                                                ##
####################################################################################################