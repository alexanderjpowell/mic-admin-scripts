# 
# 
# 
# 
# 

from datetime import datetime, timedelta
import firebase_admin
from firebase_admin import credentials, firestore
import config

UID = 'xgdRnVu3yrgjEhrMQgDSImBEOCc2'

cred = credentials.Certificate(config.serviceAccountKey)
firebase_admin.initialize_app(cred)
db = firestore.client()

DIRECTION_DESCENDING = firestore.Query.DESCENDING

def delete_records_older_than_one_month():
	refresh_date = datetime.now() - timedelta(days=30)
	query = db.collection('scans').where('uid', '==', UID).where('timestamp', '<=', refresh_date).order_by('timestamp', direction=DIRECTION_DESCENDING).limit(50)
	docs = query.stream()
	deleted = 0
	for doc in docs:
		print('Email: ' + doc.get('email') + ', ID: ' + doc.id + ', Date: ' + str(doc.get('timestamp')))
		doc.reference.delete()
		deleted = deleted + 1
	print(str(deleted) + ' records deleted.')

def clear_deleted_user_data():
	pass

if __name__ == "__main__":

	delete_records_older_than_one_month()