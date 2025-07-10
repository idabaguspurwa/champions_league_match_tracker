import json
import boto3
from botocore.exceptions import ClientError

S3_BUCKET_NAME = "ucl-lake-2025"
s3_client = boto3.client('s3', region_name='ap-southeast-1')

def fix_json_file(s3_key):
    """Download JSON file, remove indentation, and re-upload as single line"""
    try:
        # Download the file
        print(f"Processing {s3_key}...")
        response = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=s3_key)
        content = response['Body'].read().decode('utf-8')
        
        # Parse and re-serialize without indentation
        data = json.loads(content)
        single_line_json = json.dumps(data)
        
        # Upload back to S3
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=s3_key,
            Body=single_line_json
        )
        print(f"✓ Fixed {s3_key}")
        return True
        
    except Exception as e:
        print(f"✗ Error fixing {s3_key}: {e}")
        return False

def main():
    """Fix JSON formatting for all raw data files"""
    print("Fixing JSON formatting for raw data files...")
    
    # List all objects in raw/ folder
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=S3_BUCKET_NAME, Prefix='raw/')
        
        json_files = []
        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    if obj['Key'].endswith('.json'):
                        json_files.append(obj['Key'])
        
        print(f"Found {len(json_files)} JSON files to fix")
        
        for json_file in json_files:
            fix_json_file(json_file)
            
        print("Done fixing JSON files!")
        
    except Exception as e:
        print(f"Error listing S3 objects: {e}")

if __name__ == "__main__":
    main()
