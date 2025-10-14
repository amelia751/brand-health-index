#!/usr/bin/env python3
"""
Script to re-process Reddit data with proper NLP enrichment for Oct 9-12
"""

import json
import gzip
import tempfile
import os
from google.cloud import storage
from datetime import datetime
import sys

try:
    from nlp_module import NLPEnricher
    print("✅ Successfully imported NLP enricher")
except ImportError as e:
    print(f"❌ Failed to import NLP enricher: {e}")
    sys.exit(1)

class NLPReprocessor:
    def __init__(self):
        self.storage_client = storage.Client(project='trendle-469110')
        self.bucket = self.storage_client.bucket('brand-health-raw-data-469110')
        self.nlp_enricher = NLPEnricher()
        
    def process_file(self, file_path: str):
        """Process a single GCS file with NLP enrichment"""
        print(f"🔄 Processing: {file_path}")
        
        # Download file
        blob = self.bucket.blob(file_path.replace('gs://brand-health-raw-data-469110/', ''))
        
        with tempfile.NamedTemporaryFile() as temp_file:
            blob.download_to_filename(temp_file.name)
            
            # Read and process data
            processed_records = []
            record_count = 0
            
            with gzip.open(temp_file.name, 'rt', encoding='utf-8') as f:
                for line in f:
                    if line.strip():
                        try:
                            record = json.loads(line.strip())
                            record_count += 1
                            
                            # Re-enrich with NLP
                            if 'text' in record and record['text']:
                                enriched = self.nlp_enricher.enrich_text(record['text'])
                                
                                # Update record with new NLP data
                                record.update({
                                    'sentiment': enriched['sentiment'],
                                    'severity': enriched['severity'], 
                                    'topics': enriched['topics'],
                                    'nlp_model': enriched['nlp_model'],
                                    'nlp_confidence': enriched['confidence'],
                                    'nlp_language': enriched['language'],
                                    'nlp_reprocessed_at': datetime.utcnow().isoformat() + 'Z'
                                })
                                
                                if 'nlp_error' in enriched:
                                    record['nlp_error'] = enriched['nlp_error']
                            
                            processed_records.append(record)
                            
                        except json.JSONDecodeError as e:
                            print(f"⚠️  JSON decode error: {e}")
                            continue
            
            print(f"📊 Processed {record_count} records")
            
            # Upload back to GCS
            if processed_records:
                with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl') as output_file:
                    for record in processed_records:
                        output_file.write(json.dumps(record) + '\n')
                    output_file.flush()
                    
                    # Compress and upload
                    with tempfile.NamedTemporaryFile(suffix='.jsonl.gz') as compressed_file:
                        with open(output_file.name, 'rb') as f_in:
                            with gzip.open(compressed_file.name, 'wb') as f_out:
                                f_out.writelines(f_in)
                        
                        # Upload back to same location
                        blob.upload_from_filename(compressed_file.name)
                        print(f"✅ Updated: {file_path}")
                        
                        # Show sample sentiment
                        sample_sentiments = [r.get('sentiment', 'N/A') for r in processed_records[:5]]
                        print(f"📈 Sample sentiments: {sample_sentiments}")
    
    def process_date_range(self, dates):
        """Process all files for given dates"""
        for date in dates:
            print(f"\n📅 Processing date: {date}")
            prefix = f"raw/reddit/dt={date}/"
            
            blobs = list(self.bucket.list_blobs(prefix=prefix))
            if not blobs:
                print(f"  No files found for {date}")
                continue
                
            for blob in blobs:
                if blob.name.endswith('.jsonl.gz'):
                    file_path = f"gs://brand-health-raw-data-469110/{blob.name}"
                    try:
                        self.process_file(file_path)
                    except Exception as e:
                        print(f"❌ Error processing {file_path}: {e}")
                        continue

def main():
    print("🚀 Starting NLP re-processing for Oct 9-12...")
    
    dates_to_process = [
        "2025-10-09",
        "2025-10-10", 
        "2025-10-11",
        "2025-10-12"
    ]
    
    processor = NLPReprocessor()
    processor.process_date_range(dates_to_process)
    
    print("\n✅ NLP re-processing complete!")

if __name__ == "__main__":
    main()
