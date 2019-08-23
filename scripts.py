# 
# 
# 
# 
# 

from datetime import datetime, timedelta
import firebase_admin
from firebase_admin import credentials, firestore
from firebase_admin import auth
import config

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

def clear_upload_file_data(uid, batch_size):
	coll_ref = db.collection('formUploads/' + uid + '/uploadFormData')
	docs = coll_ref.limit(batch_size).stream()
	deleted = 0

	for doc in docs:
		print('ID: ' + doc.id)
		doc.reference.delete()
		deleted = deleted + 1

	if deleted >= batch_size:
		return clear_upload_file_data(uid, batch_size)

def clear_deleted_user_data():
	pass

def list_users():
	page = auth.list_users()
	while page:
		for user in page.users:
			print('UID: {0}, Name: {1}, Email: {2}, Email verified: {3}'.format(user.uid, user.display_name, user.email, user.email_verified))
		page = page.get_next_page()

def list_premium_users():
	pass

def revoke_user_token(uid):
	auth.revoke_refresh_tokens(uid)
	user = auth.get_user(uid)

def revoke_all_user_tokens():
	page = auth.list_users()
	while page:
		for user in page.users:
			auth.revoke_refresh_tokens(user.uid)
			user = auth.get_user(user.uid)
		page = page.get_next_page()

if __name__ == "__main__":

	#delete_records_older_than_one_month()

	'''query = db.collection('formUploads/<UID>/uploadFormData')

	docs = query.stream()
	for doc in docs:
		query.document(doc.id).update({'timestamp' : datetime.now() + timedelta(hours=4), 'isCompleted' : False})
		#print(doc.to_dict())'''

	user = auth.get_user_by_email('lotrrox@gmail.com')
	print(user.displayName)














