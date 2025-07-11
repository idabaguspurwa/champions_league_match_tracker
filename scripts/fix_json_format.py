import json
import boto3
from botocore.exceptions import ClientError

S3_BUCKET_NAME = "ucl-lake-2025"
s3_client = boto3.client('s3', region_name='ap-southeast-1')

def fix_json_file(s3_key):
    """Download JSON file, ensure proper formatting, and re-upload as single line"""
    try:
        # Download the file
        print(f"Processing {s3_key}...")
        response = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=s3_key)
        content = response['Body'].read().decode('utf-8')
        
        # Try to parse and validate JSON
        try:
            data = json.loads(content)
        except json.JSONDecodeError as e:
            print(f"⚠️  JSON parsing error in {s3_key}: {e}")
            # Try to fix common JSON issues
            content = content.strip()
            if not content.startswith('{') and not content.startswith('['):
                print(f"⚠️  File doesn't start with valid JSON: {s3_key}")
                return False
            try:
                data = json.loads(content)
            except json.JSONDecodeError:
                print(f"✗ Cannot fix JSON in {s3_key}")
                return False
        
        # Re-serialize as single line JSON (no indentation)
        single_line_json = json.dumps(data, separators=(',', ':'))
        
        # Verify the result is still valid JSON
        try:
            json.loads(single_line_json)
        except json.JSONDecodeError:
            print(f"✗ Generated invalid JSON for {s3_key}")
            return False
        
        # Only upload if content has changed
        if content != single_line_json:
            s3_client.put_object(
                Bucket=S3_BUCKET_NAME,
                Key=s3_key,
                Body=single_line_json,
                ContentType='application/json'
            )
            print(f"✓ Fixed {s3_key} (reduced from {len(content)} to {len(single_line_json)} chars)")
        else:
            print(f"✓ {s3_key} already properly formatted")
        
        return True
        
    except Exception as e:
        print(f"✗ Error processing {s3_key}: {e}")
        return False

def main():
    """Fix JSON formatting for all raw data files"""
    print("Fixing JSON formatting for raw data files...")
    
    try:
        # List all objects in raw/ folder
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=S3_BUCKET_NAME, Prefix='raw/')
        
        json_files = []
        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    if obj['Key'].endswith('.json'):
                        json_files.append(obj['Key'])
        
        if not json_files:
            print("No JSON files found in raw/ folder")
            return
        
        print(f"Found {len(json_files)} JSON files to process")
        
        success_count = 0
        for json_file in json_files:
            if fix_json_file(json_file):
                success_count += 1
        
        print(f"\nCompleted! Successfully processed {success_count}/{len(json_files)} files")
        
        # Also create a summary file
        summary = {
            "processing_timestamp": "2025-01-01T00:00:00Z",
            "total_files": len(json_files),
            "successfully_processed": success_count,
            "files_processed": json_files[:10]  # First 10 files as sample
        }
        
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key='raw/json_processing_summary.json',
            Body=json.dumps(summary, separators=(',', ':')),
            ContentType='application/json'
        )
        
    except Exception as e:
        print(f"Error listing S3 objects: {e}")

if __name__ == "__main__":
    main()
