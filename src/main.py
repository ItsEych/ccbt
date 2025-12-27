import os
import json
from datetime import datetime, timezone
from appwrite.client import Client
from appwrite.services.databases import Databases
from appwrite.query import Query
import feedparser
import hashlib

def main(context):
    # Setup Appwrite
    client = Client()
    client.set_endpoint(os.environ['APPWRITE_ENDPOINT'])
    client.set_project(os.environ['APPWRITE_PROJECT_ID'])
    client.set_key(os.environ['APPWRITE_API_KEY'])
    
    databases = Databases(client)
    db_id = os.environ['APPWRITE_DATABASE_ID']
    
    results = {"fetched": 0, "new_items": 0, "errors": []}
    
    try:
        # دریافت منابع فعال
        sources = databases.list_documents(
            database_id=db_id,
            collection_id='sources',
            queries=[Query.equal('is_active', True)]
        )
        
        for source in sources['documents']:
            try:
                # خواندن RSS
                feed = feedparser.parse(source['url'])
                results['fetched'] += 1
                
                # پردازش هر آیتم
                for entry in feed.entries[:5]:  # فقط 5 تای اخیر
                    # ساخت GUID یونیک
                    guid = entry.get('id') or entry.get('link') or hashlib.md5(
                        entry.get('title', '').encode()
                    ).hexdigest()
                    
                    # چک کردن تکراری نبودن
                    existing = databases.list_documents(
                        database_id=db_id,
                        collection_id='content_items',
                        queries=[Query.equal('item_guid', guid)]
                    )
                    
                    if existing['total'] == 0:
                        # ذخیره آیتم جدید
                        content_doc = databases.create_document(
                            database_id=db_id,
                            collection_id='content_items',
                            document_id='unique()',
                            data={
                                'source_id': source['$id'],
                                'item_guid': guid,
                                'title': entry.get('title', 'No Title')[:1000],
                                'content': entry.get('summary', '')[:10000],
                                'link': entry.get('link', ''),
                                'published_date': datetime.now(timezone.utc).isoformat(),
                                'fetched_at': datetime.now(timezone.utc).isoformat()
                            }
                        )
                        
                        # اضافه به صف انتشار تلگرام
                        formatted = f"📰 {content_doc['title']}

{content_doc['content'][:300]}...

🔗 {content_doc['link']}

#news #automation"
                        
                        databases.create_document(
                            database_id=db_id,
                            collection_id='publish_queue',
                            document_id='unique()',
                            data={
                                'content_id': content_doc['$id'],
                                'platform': 'telegram',
                                'status': 'pending',
                                'formatted_content': formatted,
                                'retry_count': 0,
                                'scheduled_at': datetime.now(timezone.utc).isoformat()
                            }
                        )
                        results['new_items'] += 1
                
                # آپدیت last_fetched
                databases.update_document(
                    database_id=db_id,
                    collection_id='sources',
                    document_id=source['$id'],
                    data={'last_fetched': datetime.now(timezone.utc).isoformat()}
                )
                
            except Exception as e:
                results['errors'].append(f"Source {source['name']}: {str(e)}")
        
    except Exception as e:
        results['errors'].append(f"Main error: {str(e)}")
    
    return context.res.json(results)
