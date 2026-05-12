import boto3
import xml.etree.ElementTree as ET
import os
import re
import csv
from datetime import datetime
import logging

# Import rights validation functions
from validate_rights_uri import validate_rights_uri, get_rights_info

# Add a timestamp to the log file name
log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
os.makedirs(log_dir, exist_ok=True)
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_filename = os.path.join(log_dir, f'dates_debug_{timestamp}.log')
logging.basicConfig(
    filename=log_filename,
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)s %(message)s'
)

# Set up warning file for identifier issues and fallbacks
multiple_identifiers_warning_file = os.path.join(log_dir, f'identifier_warnings_{timestamp}.txt')

# Set up file for tracking invalid rights URIs
env_name = os.getenv("ENV", "unknown")  # Get environment (prod/preprod)
invalid_rights_uris_file = os.path.join(log_dir, f'invalid_rights_uris_{env_name}_{timestamp}.txt')
invalid_rights_uris_csv_file = os.path.join(log_dir, f'invalid_rights_uris_{env_name}_{timestamp}.csv')
invalid_rights_uris_list = []  # Track all invalid URIs found during processing

# Function to correct common rights URI issues
def correct_rights_uri(uri):
    """
    Correct common issues in rights URIs.
    
    1. Extract URL from HTML/paragraph content if present
    2. Replace /page/ with /vocab/ in rightsstatements.org URLs
    3. Remove query parameters like ?language=en
    
    Args:
        uri: The original URI (may contain HTML/paragraph text)
        
    Returns:
        Corrected URI string
    """
    if not uri or uri == '(empty)':
        return ''
    
    # Extract URL from HTML content if present
    # Look for URLs in href attributes or plain URLs in text
    # Match both http:// and https:// for rightsstatements.org and creativecommons.org
    # Pattern captures full path including rights code (e.g., InC-EDU) and version (1.0 or 4.0)
    url_pattern = r'https?://(?:rightsstatements\.org|creativecommons\.org)[^\s<>"?]*?/(?:1\.0|4\.0)/?'
    matches = re.findall(url_pattern, uri)
    
    if matches:
        # Use the last match (typically the actual URL)
        uri = matches[-1]
    
    # Replace /page/ with /vocab/ in rightsstatements.org URLs (handles both http and https)
    uri = re.sub(r'(https?://rightsstatements\.org)/page/', r'\1/vocab/', uri)
    
    # Remove query parameters
    if '?' in uri:
        uri = uri.split('?')[0]
    
    # Ensure trailing slash
    if not uri.endswith('/'):
        uri += '/'
    
    return uri

# DEBUG: Script started
print('DEBUG: Starting dpla_xmloutput.py')
s3_prefix_config = os.getenv("S3_PREFIX")
if s3_prefix_config:
    print(f'       Filtering enabled: S3_PREFIX="{s3_prefix_config}" + visibility=True')
else:
    print('       Filtering enabled: visibility=True only (no S3 filtering)')

# DynamoDB setup
# Testing with preproduction first and then run it on production
# JLG 09/08/2025
# This script exports DynamoDB records to XML files for use in DPLA ingestion.
# It assumes a specific schema in DynamoDB and maps fields to XML elements.

env = {}
env["region_name"] = "set in .sh file"
env["COLLECTION_IDENTIFIER"] = os.getenv("COLLECTION_IDENTIFIER")
env["REGION"] = os.getenv("REGION")
env["DYNAMODB_TABLE"] = os.getenv("DYNAMODB_TABLE")
env["COLLECTION_TABLE"] = os.getenv("COLLECTION_TABLE")
env["FOLDER_MAPPING_TABLE"] = os.getenv("FOLDER_MAPPING_TABLE", "identifier-folder-mapping-for-dpla-xml-export")
env["LONG_URL_PATH"] = os.getenv("LONG_URL_PATH")
env["TYPE"] = os.getenv("TYPE")

# DEBUG: Print environment variables
print(f'DEBUG: Environment variables loaded: {env}')

# Check for missing environment variables
for key in ["COLLECTION_IDENTIFIER", "REGION", "DYNAMODB_TABLE", "LONG_URL_PATH", "TYPE"]:
    if not env[key]:
        print(f'WARNING: Environment variable {key} is not set!')
if not env["COLLECTION_TABLE"]:
    print('WARNING: COLLECTION_TABLE is not set — dcterms:isPartOf will be skipped')

# Setup DynamoDB resource
try:
    aws_profile = os.getenv("AWS_PROFILE")
    session = boto3.Session(profile_name=aws_profile, region_name=env["REGION"])
    dynamodb = session.resource("dynamodb")
    dbtable = dynamodb.Table(env["DYNAMODB_TABLE"])
    print(f'DEBUG: Connected to DynamoDB table: {env["DYNAMODB_TABLE"]}')
except Exception as e:
    print(f'ERROR: Failed to connect to DynamoDB: {e}')
    raise

# Load identifier -> folder mapping from DynamoDB table at startup
_folder_mapping_cache = {}  # prefix (uppercase) -> output_folder_name
try:
    folder_mapping_table = dynamodb.Table(env["FOLDER_MAPPING_TABLE"])
    response = folder_mapping_table.scan()
    for item in response.get("Items", []):
        _folder_mapping_cache[item["identifier"].upper()] = item["output_folder_name"]
    while "LastEvaluatedKey" in response:
        response = folder_mapping_table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
        for item in response.get("Items", []):
            _folder_mapping_cache[item["identifier"].upper()] = item["output_folder_name"]
    print(f'DEBUG: Loaded {len(_folder_mapping_cache)} folder mappings from "{env["FOLDER_MAPPING_TABLE"]}"')
except Exception as e:
    print(f'WARNING: Could not load folder mapping table "{env["FOLDER_MAPPING_TABLE"]}": {e}')
    print('         Falling back to hardcoded mappings if available')


# Function to collect S3 paths for all identifiers (for reporting, not filtering)
# COMMENTED OUT - S3 path collection disabled for performance
# def get_s3_paths_for_identifiers():
#     """
#     Scans S3 bucket to map identifiers to their actual S3 folder paths.
#     This is used for reporting actual S3 locations, not for filtering.
#     Returns a dict mapping identifier to actual S3 folder path.
#     """
#     s3_bucket = os.getenv("S3_BUCKET")
#     
#     if not s3_bucket:
#         print("DEBUG: S3_BUCKET not set, cannot collect S3 paths")
#         return {}
#     
#     # Scan entire bucket (or with prefix if set)
#     s3_prefix = os.getenv("S3_PREFIX", "")
#     print(f'DEBUG: Scanning S3 bucket "{s3_bucket}" to collect actual paths...')
#     
#     try:
#         s3_client = boto3.client('s3', region_name=env["REGION"])
#         paginator = s3_client.get_paginator('list_objects_v2')
#         
#         s3_paths_map = {}  # Map identifier -> actual S3 folder path
#         object_count = 0
#         
#         if s3_prefix:
#             pages = paginator.paginate(Bucket=s3_bucket, Prefix=s3_prefix)
#         else:
#             pages = paginator.paginate(Bucket=s3_bucket)
#         
#         for page in pages:
#             for obj in page.get('Contents', []):
#                 object_count += 1
#                 key = obj['Key']
#                 
#                 # Split key into parts
#                 path_parts = key.split('/')
#                 
#                 # Look for 'Access' folder in path (typical structure)
#                 if len(path_parts) >= 3 and 'Access' in path_parts:
#                     access_index = path_parts.index('Access')
#                     if access_index > 0:
#                         # The folder right before 'Access' is the identifier
#                         identifier = path_parts[access_index - 1]
#                         if identifier:
#                             # Store the actual S3 folder path for this identifier
#                             folder_path = '/'.join(path_parts[:access_index])
#                             s3_full_path = f"s3://{s3_bucket}/{folder_path}/"
#                             s3_paths_map[identifier] = s3_full_path
#         
#         print(f'DEBUG: Scanned {object_count} S3 objects, found {len(s3_paths_map)} identifiers with S3 paths')
#         return s3_paths_map
#         
#     except Exception as e:
#         print(f'ERROR: Failed to scan S3 bucket for paths: {e}')
#         return {}


# Function to get federated identifiers from S3 (for filtering)
def get_federated_identifiers_from_s3():
    """
    Read S3 bucket with S3_PREFIX and extract identifiers from object keys.
    Returns a dict mapping identifier to actual S3 folder path.
    Only used when S3_PREFIX filtering is enabled.
    """
    s3_bucket = os.getenv("S3_BUCKET")
    s3_prefix = os.getenv("S3_PREFIX")
    
    if not s3_bucket or not s3_prefix:
        print("DEBUG: S3_BUCKET or S3_PREFIX not set, skipping S3 filtering")
        return None
    
    print(f'DEBUG: Reading S3 bucket "{s3_bucket}" with prefix "{s3_prefix}"...')
    
    try:
        s3_client = boto3.client('s3', region_name=env["REGION"])
        paginator = s3_client.get_paginator('list_objects_v2')
        
        federated_identifiers = {}  # Map identifier -> S3 folder path
        object_count = 0
        
        for page in paginator.paginate(Bucket=s3_bucket, Prefix=s3_prefix):
            for obj in page.get('Contents', []):
                object_count += 1
                key = obj['Key']
                
                # Remove the prefix to get the relative path
                relative_key = key[len(s3_prefix):] if key.startswith(s3_prefix) else key
                
                if not relative_key or not '/' in relative_key:
                    continue
                
                # Extract identifier from folder structure
                # Expected structure: top_folder/identifier_folder/Access/files
                path_parts = relative_key.split('/')
                
                if len(path_parts) >= 3 and 'Access' in path_parts:
                    access_index = path_parts.index('Access')
                    if access_index > 0:
                        # The folder right before 'Access' is the identifier
                        identifier = path_parts[access_index - 1]
                        if identifier:
                            # Store the actual S3 folder path for this identifier
                            s3_folder_path = f"s3://{s3_bucket}/{s3_prefix}{'/'.join(path_parts[:access_index])}/"
                            federated_identifiers[identifier] = s3_folder_path
        
        print(f'DEBUG: Found {object_count} S3 objects in prefix "{s3_prefix}"')
        print(f'DEBUG: Extracted {len(federated_identifiers)} unique identifiers from S3')
        
        if federated_identifiers:
            sample = list(federated_identifiers.keys())[:5]
            print(f'DEBUG: Sample identifiers from S3: {sample}')
        
        return federated_identifiers
        
    except Exception as e:
        print(f'ERROR: Failed to read S3 bucket: {e}')
        print(f'       Continuing without S3 filtering...')
        return None


# Namespace mapping
NSMAP = {
    "dc": "http://purl.org/dc/elements/1.1/",
    "dcterms": "http://purl.org/dc/terms/",
    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "edm": "http://www.europeana.eu/schemas/edm/",
    "xsi": "http://www.w3.org/2001/XMLSchema-instance",
    None: "http://dplava.lib.virginia.edu"
}
# DEBUG: Registering XML namespaces
for prefix, uri in NSMAP.items():
    if prefix:  # skip default namespace
        ET.register_namespace(prefix, uri)
        #rint(f'DEBUG: Registered namespace {prefix}: {uri}')

# Function to look up ISO 639-2 code from language codes DynamoDB table
def get_iso_639_2_code(iso_639_1):
    """
    Look up the ISO 639-2 code from the DynamoDB table.
    Returns the 3-letter code if found, else returns the original value.
    """
    try:
        region = os.getenv("REGION")
        lang_table_name = os.getenv("LANGUAGE_CODES_TABLE")
        dynamodb_lang = boto3.resource("dynamodb", region_name=region)
        lang_table = dynamodb_lang.Table(lang_table_name)
        response = lang_table.get_item(Key={'iso_639_1': iso_639_1})
        return response['Item']['iso_639_2']
    except Exception as e:
        print(f"WARNING: Could not map language code '{iso_639_1}': {e}")
        return iso_639_1

def get_permalink(item):
    long_url_path = os.getenv("LONG_URL_PATH")
    item_type = os.getenv("TYPE")
    custom_key = item.get("custom_key", "")
    noid = custom_key.split("/")[-1] if custom_key else ""
    if long_url_path and item_type and noid:
        return f"{long_url_path.rstrip('/')}/{item_type}/{noid}"
    print(f"WARNING: Could not construct permalink for item: long_url_path={long_url_path}, item_type={item_type}, noid={noid}")
    return ""


# Cache for collection UUID -> title lookups to avoid repeated DynamoDB calls
_collection_cache = {}

def get_collection_identifier(collection_uuid):
    """
    Look up a collection's title from the Collection DynamoDB table by its UUID.
    Maps heirarchy_path UUID -> collection table id -> title field.
    Results are cached in-memory so each UUID is only fetched once per run.
    Returns the title string, or None if not found.
    """
    if not collection_uuid:
        return None

    # Return cached value if already looked up
    if collection_uuid in _collection_cache:
        return _collection_cache[collection_uuid]

    collection_table_name = os.getenv("COLLECTION_TABLE")
    if not collection_table_name:
        print("WARNING: COLLECTION_TABLE env var not set; cannot look up isPartOf")
        _collection_cache[collection_uuid] = None
        return None

    try:
        region = os.getenv("REGION")
        dynamodb_coll = boto3.resource("dynamodb", region_name=region)
        coll_table = dynamodb_coll.Table(collection_table_name)
        response = coll_table.get_item(Key={"id": collection_uuid})
        coll_item = response.get("Item")
        if coll_item:
            title = coll_item.get("title")
            _collection_cache[collection_uuid] = title
            return title
        else:
            print(f"WARNING: No collection found for UUID '{collection_uuid}'")
            _collection_cache[collection_uuid] = None
            return None
    except Exception as e:
        print(f"WARNING: Could not look up collection '{collection_uuid}': {e}")
        _collection_cache[collection_uuid] = None
        return None


def process_rights_statement(rights_uri, item_id):
    """
    Validate and enrich rights statement information from the RightsStatement lookup table.
    
    Args:
        rights_uri: The rights URI from the item metadata
        item_id: The item identifier for logging
        
    Returns:
        Dictionary with:
            - 'valid': Boolean indicating if URI is valid
            - 'uri': The original URI
            - 'label': The human-readable label (e.g., "No Copyright - United States")
            - 'description': Full description text
            - 'code': Short code (e.g., "NoC-US")
            - 'category': Category (In Copyright, No Copyright, Other)
            - 'error': Error message if invalid
    """
    result = {
        'valid': False,
        'uri': rights_uri,
        'label': None,
        'description': None,
        'code': None,
        'category': None,
        'error': None
    }
    
    if not rights_uri:
        result['error'] = "Rights URI is empty"
        logging.warning(f"Item {item_id}: No rights URI provided")
        return result
    
    # Validate the URI
    is_valid, code, error = validate_rights_uri(rights_uri)
    
    if not is_valid:
        result['error'] = error
        logging.error(f"Item {item_id}: Invalid rights URI '{rights_uri}' - {error}")
        return result
    
    # Get full rights information
    rights_info = get_rights_info(rights_uri)
    
    if rights_info:
        result['valid'] = True
        result['label'] = rights_info.get('RightsLabel')
        result['description'] = rights_info.get('RightsDescription')
        result['code'] = rights_info.get('RightsCode')
        result['category'] = rights_info.get('RightsCategory')
        logging.info(f"Item {item_id}: Valid rights statement - {code}")
    else:
        result['error'] = "Could not retrieve rights information"
        logging.error(f"Item {item_id}: Could not retrieve rights info for '{rights_uri}'")
    
    return result

def clean_text_for_xml(text):
    """
    Clean text by removing backslash escaping before XML generation.
    ElementTree handles <, >, & escaping automatically.
    Our serialization handles quote/apostrophe escaping with &quot; and &apos;.
    """
    if not text:
        return text
    
    text_str = str(text)
    
    # Remove backslash-escaped quotes from source data
    # DynamoDB has \"  which should become just "
    # Then our serialization will convert " to &quot;
    text_str = text_str.replace('\\"', '"')
    text_str = text_str.replace("\\'", "'")
    
    return text_str

def is_likely_date(s):
    # Accepts YYYY-MM-DD, YYYY-MM, YYYY/MM/DD, YYYY/MM, or 4-digit year
    s = s.strip()
    if len(s) == 4 and s.isdigit():
        return True
    try:
        datetime.strptime(s, "%Y-%m-%d")
        return True
    except Exception:
        pass
    try:
        datetime.strptime(s, "%Y-%m")
        return True
    except Exception:
        pass
    try:
        datetime.strptime(s, "%Y/%m/%d")
        return True
    except Exception:
        pass
    try:
        datetime.strptime(s, "%Y/%m")
        return True
    except Exception:
        pass
    return False

def build_xml(item):
    """
    Build XML for a single DynamoDB row.
    """
    print(f'DEBUG: Building XML for item: {item.get("identifier", "NO IDENTIFIER FOUND")}')
    # Create root with minimal attributes, custom serialization will handle formatting
    root = ET.Element("mdRecord")
    # Add xsi:schemaLocation attribute
    root.set(f"{{{NSMAP['xsi']}}}schemaLocation", "http://dplava.lib.virginia.edu dplava.xsd")

    # Add dcterms fields in specific order with date appearing after subject
    # Fields before date
    fields_before_date = [
        "identifier", "title", "description", "language", "contributor", "subject"
    ]
    
    for field in fields_before_date:
        if field in item:
            value = item[field]
            if isinstance(value, list):
                for v in value:
                    if field == "language":
                        v = get_iso_639_2_code(v)
                    # Clean text to remove backslash escaping before assigning
                    cleaned_text = clean_text_for_xml(v)
                    ET.SubElement(root, f"{{{NSMAP['dcterms']}}}{field}").text = cleaned_text
            else:
                if field == "language":
                    value = get_iso_639_2_code(value)
                # Clean text to remove backslash escaping before assigning
                cleaned_text = clean_text_for_xml(value)
                ET.SubElement(root, f"{{{NSMAP['dcterms']}}}{field}").text = cleaned_text

    # Add date as dcterms:date element (immediately after subject elements)
    # Source field in DynamoDB is 'display_date' — a free-text string set by curators.
    # Output as dcterms:date to match the original metadata.
    date_value = item.get("display_date")
    if date_value:
        if isinstance(date_value, list):
            # Join all list items with comma
            date_value = ", ".join([str(d).strip() for d in date_value if d and str(d).strip()])
        if date_value and isinstance(date_value, str):
            date_value = date_value.strip()
            if date_value:  # Only add if not empty after stripping
                cleaned_date = clean_text_for_xml(date_value)
                ET.SubElement(root, f"{{{NSMAP['dcterms']}}}date").text = cleaned_date
    
    # Fields after date
    fields_after_date = [
        "type", "spatial", "medium", "format"
    ]
    
    for field in fields_after_date:
        if field in item:
            value = item[field]
            if isinstance(value, list):
                for v in value:
                    # Clean text to remove backslash escaping before assigning
                    cleaned_text = clean_text_for_xml(v)
                    ET.SubElement(root, f"{{{NSMAP['dcterms']}}}{field}").text = cleaned_text
            else:
                # Clean text to remove backslash escaping before assigning
                cleaned_text = clean_text_for_xml(value)
                ET.SubElement(root, f"{{{NSMAP['dcterms']}}}{field}").text = cleaned_text

    # Add dcterms:isPartOf
    # Priority 1: Use is_part_of field from database if it exists
    # Priority 2: Fall back to walking heirarchy_path UUIDs and looking up each
    #             in the Collection table (matched on id), then emitting the title value.
    # Note: field is spelled 'heirarchy_path' in DynamoDB (preserving source typo).
    is_part_of = item.get("is_part_of")
    
    if is_part_of:
        # Use the is_part_of value directly from the database
        if isinstance(is_part_of, list):
            for value in is_part_of:
                if value:
                    ET.SubElement(root, f"{{{NSMAP['dcterms']}}}isPartOf").text = clean_text_for_xml(value)
        else:
            ET.SubElement(root, f"{{{NSMAP['dcterms']}}}isPartOf").text = clean_text_for_xml(is_part_of)
    else:
        # Fall back to heirarchy_path lookup
        heirarchy_path = item.get("heirarchy_path") or []
        if isinstance(heirarchy_path, str):
            heirarchy_path = [heirarchy_path]
        for path_uuid in heirarchy_path:
            coll_identifier = get_collection_identifier(path_uuid)
            if coll_identifier:
                ET.SubElement(root, f"{{{NSMAP['dcterms']}}}isPartOf").text = clean_text_for_xml(coll_identifier)

    # Process rights statement with validation only (no enrichment in XML output)
    print(f"  → Checking for rights field...")
    if "rights" in item:
        rights_value = item["rights"]
        # Handle list (take first value) or single value
        rights_uri = rights_value[0] if isinstance(rights_value, list) and rights_value else rights_value
        
        if rights_uri:
            print(f"  → 🔍 Rights URL found: {rights_uri}", flush=True)
            
            # Validate rights URI against RightsStatement table (using ORIGINAL URI)
            print(f"  → 📊 Checking RightsStatement lookup table...", flush=True)
            rights_data = process_rights_statement(rights_uri, item.get("identifier", "UNKNOWN"))
            
            if rights_data['valid']:
                # Valid URI - output just the URI
                print(f"  → ✅ VALIDATED! Found in table as: {rights_data['code']}", flush=True)
                rights_elem = ET.SubElement(root, f"{{{NSMAP['dcterms']}}}rights")
                rights_elem.text = rights_uri
                
                logging.info(f"Item {item.get('identifier')}: Valid rights URI - {rights_data['code']}")
            else:
                # Invalid rights URI - log error and still output the URI
                print(f"  → ❌ VALIDATION FAILED: {rights_data['error']}", flush=True)
                logging.error(f"RIGHTS VALIDATION FAILED - Item {item.get('identifier')}: {rights_data['error']}")
                
                # Get S3 path from federated_identifiers (collected from S3_PREFIX)
                identifier = item.get('identifier', 'UNKNOWN')
                s3_path = federated_identifiers.get(identifier, 'N/A') if federated_identifiers else 'N/A'
                
                # Track invalid URI for summary report (xml_filename will be added later)
                invalid_rights_uris_list.append({
                    'item_id': item.get('identifier', 'UNKNOWN'),
                    'identifier': identifier,
                    'title': item.get('title', 'N/A'),
                    'description': item.get('description', 'N/A'),
                    'uri': rights_uri,
                    'error': rights_data['error'],
                    'xml_filename': None,  # Will be populated after filename is determined
                    'item_category': item.get('item_category', 'N/A'),
                    'visibility': item.get('visibility', 'N/A'),
                    's3_path': s3_path
                })
                
                # Still add the URI to XML (for completeness) but it's been flagged in logs
                rights_elem = ET.SubElement(root, f"{{{NSMAP['dcterms']}}}rights")
                rights_elem.text = clean_text_for_xml(rights_uri)
        else:
            print(f"  → ⚠️  Rights field exists but URI is empty", flush=True)
            
            # Get S3 path from federated_identifiers (collected from S3_PREFIX)
            identifier = item.get('identifier', 'UNKNOWN')
            s3_path = federated_identifiers.get(identifier, 'N/A') if federated_identifiers else 'N/A'
            
            # Track empty rights field
            invalid_rights_uris_list.append({
                'item_id': item.get('identifier', 'UNKNOWN'),
                'identifier': identifier,
                'title': item.get('title', 'N/A'),
                'description': item.get('description', 'N/A'),
                'uri': '(empty)',
                'error': 'Rights field exists but URI is empty',
                'xml_filename': None,
                'item_category': item.get('item_category', 'N/A'),
                'visibility': item.get('visibility', 'N/A'),
                's3_path': s3_path
            })
    else:
        print(f"  → ℹ️  No 'rights' field in this item (skipping validation)", flush=True)

    # Always add provenance as required by DPLA
    ET.SubElement(
        root,
        f"{{{NSMAP['dcterms']}}}provenance"
    ).text = "Virginia Polytechnic Institute and State University. University Libraries"


    # edm fields
    # edm:isShownAt (permalink)
    permalink = get_permalink(item)
    if permalink:
        ET.SubElement(root, f"{{{NSMAP['edm']}}}isShownAt").text = permalink

    # edm:preview (thumbnail)
    thumbnail_path = item.get("thumbnail_path", "")
    if thumbnail_path:
        ET.SubElement(root, f"{{{NSMAP['edm']}}}preview").text = thumbnail_path

    # Add creator element if present
    creator = item.get("creator")
    if creator:
        if isinstance(creator, list):
            for c in creator:
                cleaned_creator = clean_text_for_xml(c)
                ET.SubElement(root, f"{{{NSMAP['dcterms']}}}creator").text = cleaned_creator
        else:
            cleaned_creator = clean_text_for_xml(creator)
            ET.SubElement(root, f"{{{NSMAP['dcterms']}}}creator").text = cleaned_creator
        
    print(f'DEBUG: Finished building XML for item: {item.get("identifier", "NO IDENTIFIER FOUND")}')
    return root

# Query all items from DynamoDB (scan example, not efficient for big tables)
# Scan for all Federated and do each collection individually and put in collection folders
# JLG 09/08/2025

print('DEBUG: Scanning DynamoDB table for items (with pagination)...')
items = []
try:
    response = dbtable.scan()
    items.extend(response.get("Items", []))
    print(f'DEBUG: Retrieved {len(response.get("Items", []))} items from first scan.')
    while 'LastEvaluatedKey' in response:
        print('DEBUG: Fetching next page of results...')
        response = dbtable.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        items.extend(response.get("Items", []))
        print(f'DEBUG: Retrieved {len(response.get("Items", []))} items from next scan. Total so far: {len(items)}')
    print(f'DEBUG: Total items retrieved from DynamoDB: {len(items)}')
except Exception as e:
    print(f'ERROR: Failed to scan DynamoDB table: {e}')
    items = []


# Output folder logic based on identifier
# Write directly to repo root
output_base_dir = os.path.dirname(os.path.abspath(__file__))
print(f'DEBUG: Output base directory set to repo root: {output_base_dir}')

# Get federated identifiers from S3
print()
print('='*70)
s3_prefix_check = os.getenv("S3_PREFIX")
if s3_prefix_check:
    print('S3-BASED FEDERATED FILTERING')
else:
    print('NO S3 FILTERING (S3_PREFIX not set or empty)')
print('='*70)
federated_identifiers = get_federated_identifiers_from_s3()

# Use federated_identifiers for S3 path lookups (already collected from S3)


# Filter and sort items for squires only
#items = [item for item in items if item.get("identifier", "").upper().startswith("SQI")]
#print(f'DEBUG: Filtered items for squires only, count: {len(items)}')
#items = sorted(items, key=lambda x: x.get("identifier", ""))
#print('DEBUG: Items sorted by identifier for output order.')

# Filter by identifier prefix if specified
filter_prefix = os.getenv("IDENTIFIER_PREFIX", None)  # Set in your .sh script
if filter_prefix:
    items = [item for item in items if item.get("identifier", "").upper().startswith(filter_prefix.upper())]
    print(f'DEBUG: Filtered items for {filter_prefix} only, count: {len(items)}')
else:
    print(f'DEBUG: Processing all items, count: {len(items)}')

# FEDERATED FILTERING: Filter by S3 identifiers
if federated_identifiers:
    print()
    print('='*70)
    print('FEDERATED ITEMS FILTERING (via S3_PREFIX)')
    print('='*70)
    print(f'DEBUG: Filtering DynamoDB items to match S3 federated identifiers...')
    initial_count = len(items)
    items = [item for item in items if item.get('identifier') in federated_identifiers]
    filtered_count = len(items)
    print(f'DEBUG: Filtered from {initial_count} to {filtered_count} items')
    print(f'       ({initial_count - filtered_count} items excluded: not in S3 federated prefix)')
    print('='*70)
else:
    print('DEBUG: No S3 filtering applied (S3_PREFIX not set or S3 was empty)')

# VISIBILITY FILTERING: Only process items with visibility=True
print()
print('='*70)
print('VISIBILITY FILTERING')
print('='*70)
print('DEBUG: Filtering for items with visibility=True...')
initial_count = len(items)
items = [item for item in items if item.get('visibility') == True]
filtered_count = len(items)
print(f'DEBUG: Filtered from {initial_count} to {filtered_count} items')
print(f'       ({initial_count - filtered_count} items excluded: visibility=False or missing)')
print('='*70)
print()
# Mapping for identifier prefixes to folders/subfolders
def get_output_subdir(identifier):
    """
    Map identifier to output folder using the DynamoDB folder mapping table.
    Prefixes are matched longest-first to handle overlapping prefixes (e.g. CIDA_ARC vs CIDA).
    Falls back to 'other' if no match is found.
    """
    identifier_upper = identifier.upper()

    # Look up in DynamoDB-loaded mapping cache, longest prefix first
    for prefix in sorted(_folder_mapping_cache.keys(), key=len, reverse=True):
        if identifier_upper.startswith(prefix.upper()):
            return _folder_mapping_cache[prefix]

    return "other"

def indent(elem, level=0):
    i = "\n" + level*"    "
    if len(elem):
        if not elem.text or not elem.text.strip():
            elem.text = i + "    "
        for e in elem:
            indent(e, level+1)
        if not e.tail or not e.tail.strip():
            e.tail = i
    else:
        if level and (not elem.tail or not elem.tail.strip()):
            elem.tail = i

for idx, item in enumerate(items):
    print(f'\nDEBUG: Processing item {idx+1}/{len(items)}')
    #rint(f'DEBUG: Raw item: {item}')
    xml_root = build_xml(item)

    # Use identifier field for folder mapping (based on prefix)
    identifier = item.get("identifier", "")
    output_subdir = get_output_subdir(identifier)
    
    # For barter collection, use other_identifier for XML filename
    # For all other collections, use identifier field for file naming
    if output_subdir == "barter":
        identifier_value = item.get("other_identifier")
        # Handle list (take first value) or single value
        if isinstance(identifier_value, list):
            identifier_value = identifier_value[0] if identifier_value else None
        if not identifier_value:
            # Fall back to identifier if other_identifier is missing
            identifier_value = item.get("identifier")
            print(f'DEBUG: Barter item missing other_identifier, using identifier instead')
    else:
        identifier_value = item.get("identifier")

    if identifier_value:
        file_identifier = identifier_value
    else:
        # Final fallback to item number if identifier is missing
        file_identifier = f"item_{idx+1}"
        warning_msg = (
            f"WARNING: Item missing identifier, using generated name\n"
            f"  Generated filename: item_{idx+1}\n"
            f"  title: {item.get('title', 'N/A')}\n"
            f"  {'-'*60}\n"
        )
        print(warning_msg)
        with open(multiple_identifiers_warning_file, 'a', encoding='utf-8') as f:
            f.write(warning_msg)
    
    print(f'DEBUG: File identifier (for filename): {file_identifier}')
    file_name = file_identifier + ".xml"

    # Update any invalid rights URIs for this item with the xml filename
    item_identifier = item.get("identifier", "")
    for invalid_entry in invalid_rights_uris_list:
        if invalid_entry['item_id'] == item_identifier and invalid_entry['xml_filename'] is None:
            invalid_entry['xml_filename'] = file_name
    print(f'DEBUG: Output subdir from mapping: {output_subdir}')

    output_dir = os.path.join(output_base_dir, output_subdir)
    print(f'DEBUG: Output directory set to: {output_dir}')

    os.makedirs(output_dir, exist_ok=True)
    print(f'DEBUG: Ensured output directory exists: {output_dir}')
    file_path = os.path.join(output_dir, file_name)
    print(f'DEBUG: Full file path for XML: {file_path}')

    indent(xml_root)
    print(f'DEBUG: Writing XML to file: {file_path}')
    try:
            # Custom serialization for exact root formatting
            def serialize_with_custom_root(root_elem):
                # Build the exact root opening tag
                root_tag = (
                    '<mdRecord xmlns:dc="http://purl.org/dc/elements/1.1/"\n'
                    '    xmlns:dcterms="http://purl.org/dc/terms/"\n'
                    '    xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"\n'
                    '    xmlns:edm="http://www.europeana.eu/schemas/edm/"\n'
                    '    xmlns="http://dplava.lib.virginia.edu"\n'
                    '    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"\n'
                    '    xsi:schemaLocation="http://dplava.lib.virginia.edu dplava.xsd">'
                )
                # Serialize children
                children = ET.tostring(root_elem, encoding="unicode", method="xml")
                # Remove the auto-generated root tag
                children = children[children.find('>')+1:]
                # Remove closing tag
                children = children[:children.rfind('</mdRecord>')]
                
                # Escape quotes in element text content (between > and <)
                # This regex finds text between tags and escapes quotes in it
                def escape_quotes_in_content(match):
                    text = match.group(1)
                    # Only escape if it's element content (not inside tag)
                    return '>' + text.replace('"', '&quot;').replace("'", '&apos;') + '<'
                
                # Replace quotes in text content (between > and <)
                children = re.sub(r'>([^<>]+)<', escape_quotes_in_content, children)
                
                # Compose final XML
                return f'{root_tag}\n{children}</mdRecord>'

            xml_str = serialize_with_custom_root(xml_root)
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(xml_str)
            print(f"DEBUG: Successfully generated {file_path}")
    except Exception as e:
        print(f'ERROR: Failed to write XML file {file_path}: {e}')
        print(f'DEBUG: Identifier {identifier} mapped to folder: {output_dir}')

# Write invalid rights URIs to file
if invalid_rights_uris_list:
    # Write text file
    with open(invalid_rights_uris_file, 'w', encoding='utf-8') as f:
        s3_prefix_for_report = os.getenv("S3_PREFIX")
        if s3_prefix_for_report:
            f.write("INVALID RIGHTS URIS REPORT (FILTERED: S3_PREFIX + VISIBILITY)\n")
        else:
            f.write("INVALID RIGHTS URIS REPORT (FILTERED: VISIBILITY ONLY)\n")
        f.write("=" * 80 + "\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        if s3_prefix_for_report:
            f.write(f"Filter Applied: S3_PREFIX='{s3_prefix_for_report}' AND visibility=True\n")
        else:
            f.write("Filter Applied: visibility=True (no S3 filtering)\n")
        f.write(f"S3_PREFIX: {s3_prefix_for_report if s3_prefix_for_report else 'Not set'}\n")
        f.write(f"Total Invalid URIs Found: {len(invalid_rights_uris_list)}\n")
        f.write("=" * 80 + "\n\n")
        
        for idx, invalid_item in enumerate(invalid_rights_uris_list, 1):
            f.write(f"{idx}. Item ID: {invalid_item['item_id']}\n")
            f.write(f"   XML File: {invalid_item['xml_filename']}\n")
            f.write(f"   Identifier: {invalid_item.get('identifier', 'N/A')}\n")
            f.write(f"   Title: {invalid_item.get('title', 'N/A')}\n")
            f.write(f"   Description: {invalid_item.get('description', 'N/A')}\n")
            f.write(f"   item_category: {invalid_item.get('item_category', 'N/A')}\n")
            f.write(f"   visibility: {invalid_item.get('visibility', 'N/A')}\n")
            f.write(f"   S3_PREFIX: {s3_prefix_for_report if s3_prefix_for_report else 'Not set'}\n")
            f.write(f"   URI: {invalid_item['uri']}\n")
            f.write(f"   Error: {invalid_item['error']}\n")
            f.write("\n")
        
        f.write("=" * 80 + "\n")
        f.write("NEXT STEPS:\n")
        f.write("- Review each invalid URI\n")
        f.write("- Check for typos or incorrect formatting\n")
        f.write("- Update items in DynamoDB with correct rights URIs\n")
        f.write("- Valid URIs are listed at: https://rightsstatements.org/page/1.0/\n")
    
    # Write CSV file with corrections
    with open(invalid_rights_uris_csv_file, 'w', encoding='utf-8', newline='') as csvfile:
        fieldnames = ['Identifier', 'S3 Path', 'Description', 'Title', 'URI (before correction)', 'URI (after correction)']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        writer.writeheader()
        for invalid_item in invalid_rights_uris_list:
            # Get description - handle list or string
            description = invalid_item.get('description', 'N/A')
            if isinstance(description, list):
                description = '; '.join([str(d) for d in description if d])
            
            # Get title - handle list or string
            title = invalid_item.get('title', 'N/A')
            if isinstance(title, list):
                title = '; '.join([str(t) for t in title if t])
            
            # Original URI
            original_uri = invalid_item.get('uri', '')
            
            # Corrected URI
            corrected_uri = correct_rights_uri(original_uri)
            
            writer.writerow({
                'Identifier': invalid_item.get('identifier', 'N/A'),
                'S3 Path': invalid_item.get('s3_path', 'N/A'),
                'Description': description,
                'Title': title,
                'URI (before correction)': original_uri,
                'URI (after correction)': corrected_uri
            })
    
    print(f"    CSV file generated: {invalid_rights_uris_csv_file}")

# Print summary about multiple identifiers
print("\n" + "="*70)
print("SCRIPT COMPLETE")
print("="*70)

# Summary for invalid rights URIs
if invalid_rights_uris_list:
    print(f"⚠️  INVALID RIGHTS URIS: {len(invalid_rights_uris_list)} items have invalid/empty rights URIs!")
    print(f"    Review text file: {invalid_rights_uris_file}")
    print(f"    Review CSV file:  {invalid_rights_uris_csv_file}")
else:
    print(f"✅ All rights URIs are valid!")

print()

if os.path.exists(multiple_identifiers_warning_file) and os.path.getsize(multiple_identifiers_warning_file) > 0:
    print(f"⚠️  NOTICE: Some items have identifier issues or used fallbacks!")
    print(f"    Review this file: {multiple_identifiers_warning_file}")
    print(f"    Issues may include:")
    print(f"      - Missing identifier (using generated name)")
else:
    print(f"✅ All items have identifier values. No fallbacks used.")
print("="*70)
