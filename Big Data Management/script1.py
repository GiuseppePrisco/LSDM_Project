
import random
import csv
import couchdb
import json
import hashlib

database_name = "my_new_db"


couch = couchdb.Server('http://admin:password@127.0.0.1:5984/')

if database_name in couch:
	db = couch[database_name]
else:
	db = couch.create(database_name)

locations = ['Italy', 'England', 'France', 'Germany', 'Spaign', 'Poland', 'Hungary', 'Denmark', 'Finland', 'Norway', 'Sweden', 'Portugal']

categories = ['Maths', 'Physics', 'Algebra', 'Programming', 'History', 'Geography']
tags = ['Long', 'Intensive', 'Easy', 'Interesting', 'Usefull']

count = 0

print("running")
	
with open("./NOT USEFULL/imdb_movies.csv", 'r', encoding="utf8") as file:
	csvreader = csv.reader(file, delimiter=',')
	for row in csvreader:
	
		title = row[0]
		description = row[4]
			
		for elem in title:
			if elem=="'" or elem=='"':
				title = title.replace(elem,' ')
			if 32 <= ord(elem) <= 126:
				continue
			else:
				title = title.replace(elem,'')
				
		
		for elem in description:
			if elem=="'" or elem=='"':
				description = description.replace(elem,' ')
			if 32 <= ord(elem) <= 126:
				continue
			else:
				description = description.replace(elem,'')
		
		author = random.choice(open("./NOT USEFULL/names.txt").readlines()).rstrip()
		category = categories[random.randint(0,len(categories)-1)]
		tag = []
		tag.append(tags[random.randint(0,len(tags)-1)])
		for j in range(random.randint(0,3)):
			tag.append(tags[random.randint(0,len(tags)-1)])
		
		tag = list(dict.fromkeys(tag))
		tag = str(tag)
	
		for elem in tag:
			if elem=="'":
				tag = tag.replace(elem,'"')
		
		year = str(random.randint(2000,2023))
		likes = str(random.randint(0,10000))
	
		verified='"'
		num = random.randint(0,4)
		if num==0:
			verified = '","verified": "true"'
		elif num==1:
			verified = '","verified": "false"'
	
		location = locations[count%len(locations)]
	
		count = count+1
	
		id = str(hashlib.md5(repr(random.randint(0,10000)).encode()).hexdigest())
		id = '"_id": "'+location+':'+id
		print("\nAdded document with id: "+id)
		
		post = '{'+id+'","author": "'+author+'","title": "'+title+'","category": "'+category+'","tags": '+tag+',"description": "'+description+verified+',"year": '+year+',"likes": '+likes+',"location": "'+location+'"}'
		post = json.loads(post)

		db.save(post)
		

