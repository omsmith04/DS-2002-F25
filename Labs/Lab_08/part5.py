import boto3
import requests

IMAGE_URL = "https://www.awsfzoo.com/media/DSC01156-scaled.jpg"
LOCAL_FILE = "camel.jpg"
bucket_name = "ds2002-f25-wca6eh"
object_name = "s3_bucket_lab/camel.jpg"         
expires_in = 3600       

s3 = boto3.client('s3', region_name='us-east-1')

response = requests.get(IMAGE_URL)
with open(LOCAL_FILE, 'wb') as f:
    f.write(response.content)

s3.upload_file(
    Filename=LOCAL_FILE,
    Bucket=bucket_name,
    Key=object_name    
)
response = s3.generate_presigned_url(
    'get_object',
    Params={'Bucket': bucket_name, 'Key': object_name},
    ExpiresIn=expires_in
)

# Print the final URL
print(response)