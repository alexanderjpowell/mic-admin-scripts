# Alexander Powell
# 
# To run:
# python3 scripts.py
# 

from datetime import datetime, timedelta
import firebase_admin
from firebase_admin import credentials, firestore
from firebase_admin import auth
import config
import collections
import csv
import pytz

cred = credentials.Certificate(config.serviceAccountKey)
firebase_admin.initialize_app(cred)
db = firestore.client()

utc = pytz.UTC
DIRECTION_DESCENDING = firestore.Query.DESCENDING
DIRECTION_ASCENDING = firestore.Query.ASCENDING

UID = 'xgdRnVu3yrgjEhrMQgDSImBEOCc2'

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
	users = []
	while page:
		for user in page.users:
			users.append(user)
			# print('UID: {0}, Name: {1}, Email: {2}, Email verified: {3}'.format(user.uid, user.display_name, user.email, user.email_verified))
		page = page.get_next_page()
	return users

def list_premium_users():
	pass

def revoke_user_token(uid):
	auth.revoke_refresh_tokens(uid)
	user = auth.get_user(uid)

'''
Can take up to 1 hour to take effect
'''
def revoke_all_user_tokens():
	page = auth.list_users()
	while page:
		for user in page.users:
			auth.revoke_refresh_tokens(user.uid)
			user = auth.get_user(user.uid)
		page = page.get_next_page()

FIRESTORE_BATCH_SIZE = 100000

def countAllScansForUser(uid):
	total_count = 0
	days_elapsed = 0
	query = db.collection('users/' + uid + '/scans').limit(FIRESTORE_BATCH_SIZE).order_by('timestamp', direction=DIRECTION_DESCENDING)
	docs = list(query.stream())
	count = len(docs)
	total_count += count

	if (count == 0):
		return 0

	while count:
		timestamp = docs[-1].to_dict()[u'timestamp']
		timestamp = timestamp.replace(tzinfo=None)
		query = db.collection('users/' + uid + '/scans').order_by('timestamp', direction=DIRECTION_DESCENDING).start_after({u'timestamp': timestamp}).limit(FIRESTORE_BATCH_SIZE)
		docs = list(query.stream())
		count = len(docs)
		total_count += count

	delta = datetime.now() - timestamp
	months = delta.days / 30.5
	avg_days = total_count / delta.days
	avg_months = total_count / months
	print("Average Number of Scans per Day: {scans:.2f}".format(scans = avg_days))
	print("Average Number of Scans per Month: {scans:.2f}".format(scans = avg_months))
	print("Total Scans Since Inception: " + str(total_count))
	print("Number of Days as Customer: " + str(delta.days))

	return total_count

def countAllScans():
	users = list_users()
	cumulative_count = 0
	for user in users:
		print(user.email)
		cumulative_count += countAllScansForUser(user.uid)
		print("------------------------------")
	print("Total Scans: " + str(cumulative_count))

def getMostRecentScanForUser(uid):
	query = db.collection('users/' + uid + '/scans').limit(1).order_by('timestamp', direction=DIRECTION_DESCENDING)
	docs = list(query.stream())
	if (len(docs) == 1):
		print(docs[0].to_dict()[u'timestamp'].tzinfo)
		return docs[0].to_dict()[u'timestamp']
	else:
		print("No scans found")
		return None

def getMostRecentScans():
	out = []
	for user in list_users():
		email = user.email
		print(email)
		timestamp = getMostRecentScanForUser(user.uid)
		if (timestamp is not None):
			out.append([email, timestamp])
	return out

'''
duration: days
'''
def getScansForUserInTimePeriod(user):
	# startTime = datetime.now() - timedelta(days=duration)
	startTime = datetime(2021, 11, 18, 0, 0)
	startTime = utc.localize(startTime)
	hour = 0
	num_scans_per_hour = []
	query = db.collection('users/' + user.uid + '/scans').where('timestamp', '>=', startTime).order_by('timestamp', direction=DIRECTION_ASCENDING)
	docs = list(query.stream())

	curTime = startTime + timedelta(hours=1)
	# endTime = utc.localize(datetime.now())
	endTime = datetime(2021, 12, 2, 0, 1)
	endTime = utc.localize(endTime)
	while (curTime < endTime):

		counter = 0
		for doc in docs:
			timestamp = doc.to_dict()[u'timestamp']
			if (timestamp <= curTime):
				counter += 1
			else:
				break
		# print(len(docs))
		if (counter > 0):
			del docs[:counter]

		num_scans_per_hour.append(counter)
		curTime += timedelta(hours=1)
	# print(len(num_scans_per_hour))
	# print(num_scans_per_hour)

	# print("-----")
	curTime = startTime #+ timedelta(hours=1)
	print(len(num_scans_per_hour))

	weeks = []
	days = []

	for i in range(len(num_scans_per_hour)):
		if (i != 0 and i % 24 == 0):
			# print(i)
			weeks.append(days)
			days = [num_scans_per_hour[i]]
		else:
			days.append(num_scans_per_hour[i])
		if (num_scans_per_hour[i] > 0):
			pass
			# print(curTime)
			# print(num_scans_per_hour[i])
		curTime += timedelta(hours=1)
	weeks.append(days)

	print(weeks)
	print(len(weeks))
	write_heatmap_csv(weeks, user.email)

'''
heatmap param is a list of lists containing scan frequency in hours
'''
def write_heatmap_csv(heatmap, email):
	# add day name to front of each nested list:
	days = ["thursday", "friday", "saturday", "sunday", "monday", "tuesday", "wednesday"]
	days = days + days
	formatted_heatmap = []
	for i in range(len(heatmap)):
		new_day = [days[i]] + heatmap[i]
		formatted_heatmap.append(new_day)
		# formatted_heatmap.insert(0, days[i])
	filename = "heatmaps/" + email + ".csv"
	with open(filename, "w") as csvfile:
		csvwriter = csv.writer(csvfile)
		header = ["day"]
		header.extend([str(i) + ":00" for i in range(24)])
		csvwriter.writerow(header)
		csvwriter.writerows(formatted_heatmap)

if __name__ == "__main__":

	user = auth.update_user('VX4QGsBEsAd5vPaNRfabmmGrA4f2', password='password')
	print('Sucessfully updated user: {0}'.format(user.uid))

	# for each customer:
	# read all scans in last 2 weeks in descending order
	# divide into hourly chunks
	# for user in list_users():
	# 	print(user.uid)
	# 	getScansForUserInTimePeriod(user)
		# getScansForUserInTimePeriod('1kyN8HCbC6gfZY8nNIYB1HjqRnH3')

	# getMostRecentScanForUser('xgdRnVu3yrgjEhrMQgDSImBEOCc2')
	# data = getMostRecentScans()
	# with open('most_recent.csv', 'w', newline='') as csvfile:
	# 	spamwriter = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
	# 	for row in data:
	# 		spamwriter.writerow(row)






	# query = db.collection('users/xgdRnVu3yrgjEhrMQgDSImBEOCc2/scans').order_by('timestamp', direction=DIRECTION_DESCENDING)
	# docs = list(query.stream())
	# for i in range(len(docs)):
	# 	print(docs[i].id)
	# 	if (i == len(docs) - 1):
	# 		timestamp = docs[i].to_dict()['timestamp']
	# 		print(type(timestamp))
	# 		print(timestamp.year)
	# 		print(timestamp.month)
	# 		print(timestamp.day)

	# print(len(docs))

	# query = db.collection('users/Q86qgiOu6UcrRF97OHmYeIyCT6a2/scans').where("machine_id", "==", "10693").limit(5).order_by('timestamp', direction=DIRECTION_DESCENDING)
	# docs = query.stream()
	# for doc in docs:
	# 	print(doc.id)
	# 	print(doc.to_dict()['timestamp'])
	# 	print("-----")


	# startTime = datetime(2021, 9, 13, 0, 0) + timedelta(hours=4)
	# endTime = datetime(2021, 9, 14, 0, 0) + timedelta(hours=4)

	# query = db.collection('users/Q86qgiOu6UcrRF97OHmYeIyCT6a2/scans').where('timestamp', '>=', startTime).where('timestamp', '<=', endTime).order_by('timestamp', direction=DIRECTION_ASCENDING)

	# docs = query.stream()
	# count = 0
	# for doc in docs:
	# 	# print(doc.id)
	# 	# print(doc.to_dict())
	# 	count += 1
	# print(count)

	'''all_lines = open('all_machines.txt', 'r').read().splitlines()
	scanned_lines = open('scanned_machines.txt', 'r').read().splitlines()'''
	#all_lines = file1.readlines()
	#scanned_lines = file2.readlines()

	'''ret = []
	for mid in all_lines:
		if mid not in scanned_lines:
			ret.append(mid)'''

	#for i in ret:
		#print(i.strip())
	'''all_machines = set()
	for i in all_lines:
		if i.strip() in all_machines:
			print(i.strip())
		else:
			all_machines.add(i.strip())'''
	'''print([item for item, count in collections.Counter(scanned_lines).items() if count > 1])'''

	# startTime = datetime.now() - timedelta(hours=5)
	# #query = db.collection('scans').where('uid', '==', 'uJjfFFwtnyL8YKFMBATkJW57BNZ2').where('timestamp', '>=', startTime).order_by('timestamp', direction=DIRECTION_DESCENDING)
	# query = db.collection('users/uJjfFFwtnyL8YKFMBATkJW57BNZ2/scans').where('timestamp', '>=', startTime).order_by('timestamp', direction=DIRECTION_DESCENDING)
	# docs = query.stream()
	# count = 0
	# for doc in docs:
	# 	#print(doc.get("machine_id"))
	# 	count += 1
	# print(count)

	'''data = {
		u'progressive1': u'11000',
		u'progressive2': u'850',
		u'progressive3': u'',
		u'progressive4': u'',
		u'progressive5': u'',
		u'progressive6': u'',
		u'progressive7': u'',
		u'progressive8': u'',
		u'progressive9': u'',
		u'progressive10': u'',
		u'base1': u'10000',
		u'base2': u'800',
		u'increment1': u'0.5',
		u'increment2': u'0.1',
		u'location': u'AB2301-03',
		u'machine_id': u'12345',
		u'notes': u'',
		u'timestamp': datetime.datetime.now(),
		u'userName': u'Joe'
	}

	db.collection(u'users').document(u'1kyN8HCbC6gfZY8nNIYB1HjqRnH3').collection(u'scans').document().set(data)'''

	#delete_records_older_than_one_month()

	#list_users()
	#revoke_user_token(UID)

	#revoke_all_user_tokens()

	'''query = db.collection('formUploads/<UID>/uploadFormData')

	docs = query.stream()
	for doc in docs:
		query.document(doc.id).update({'timestamp' : datetime.now() + timedelta(hours=4), 'isCompleted' : False})
		#print(doc.to_dict())'''

	#user = auth.get_user_by_email('lotrrox@gmail.com')
	#print(user.uid)

	# To revoke access tokens - this will force user to sign back in within 1 hour
	'''auth.revoke_refresh_tokens(uid)
	user = auth.get_user(uid)
	# Convert to seconds as the auth_time in the token claims is in seconds.
	revocation_second = user.tokens_valid_after_timestamp / 1000
	print('Tokens revoked at: {0}'.format(revocation_second))'''

	# To define custom claims for a user
	'''user = auth.get_user_by_email('user@admin.example.com')
	# Confirm user is verified - custom claims cannot be set unless email is verified
	if user.email_verified:
	    # Add custom claims for additional privileges.
	    # This will be picked up by the user on token refresh or next sign in on new device.
	    auth.set_custom_user_claims(user.uid, {
	        'admin': True
	    })'''

	# watch https://www.youtube.com/watch?v=3hj_r_N0qMs
	# and https://www.youtube.com/watch?v=UZ9s_20Hk3U














