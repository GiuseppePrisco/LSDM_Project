
import psycopg2
import csv
import re

# perform the connection to the database
connection = psycopg2.connect(
	database="information integration",
	user="postgres",
	password="vgcdb",
	host="localhost",
	port= "5432"
)


# list containing all the UCQs
queries = []

#########################################

#query 1
query = """SELECT DISTINCT hasPublished.name AS publisher, hasPublished.game AS game
FROM crossgame, publisher, hasPublished
WHERE crossgame.name = hasPublished.game AND publisher.name = hasPublished.name"""

header = ["publisher", "game"]

queries.append([header,query])

#########################################


#########################################

#query 2
query = """SELECT DISTINCT hasPublished.name AS publisher, hasPublished.game AS game
FROM publisher, hasPublished, game, award
WHERE game.name = hasPublished.game AND publisher.name = hasPublished.name 
AND award.game = game.name AND award.winner = '1' 
AND award.category IN ('Best VR/AR Game','Student Game Award','Best Music/Sound Design','Best Strategy Game',
'ESports Game of the Year','Best Online Experience','Best VR Game','Best Handheld Game',
'Most Anticipated Game','Best Student Game','Games for Change','Player''s Voice',
'Best Esports Game','Best Multiplayer','Most Anticipated Game 2015','Best Multiplayer Game',
'Best Score/Soundtrack','Best Action Game','Best Fighting Game','Best Score/Music','Best Remaster',
'Best Art Direction','Fresh Indie Game','Global Gaming Citizens','Best Audio Design',
'Best eSports Game','Best Independent Game','Best Shooter','Best Fan Creation','Game of the Year',
'Best Mobile/Handheld Game','Best Sports/Racing Game','Best Game Direction','Best Ongoing Game',
'Best Mobile Game','Games for Impact','Best Family Game','Best Debut Indie Game',
'Best Role Playing Game','Best Action/Adventure Game','Best Community Support',
'Best Narrative','Chinese Fan Game Award')"""

header = ["publisher", "game"]

queries.append([header,query])

#########################################


#########################################

#query 3
query = """SELECT DISTINCT crossgame.name AS game
FROM crossgame, award as award1, award as award2
WHERE award1.game = award2.game AND award1.game = crossgame.name 
AND award1.category = 'Game of the Year' AND award2.category = 'Best Action/Adventure Game' 
AND award1.winner = '1' AND award2.winner = '1'"""

header = ["game"]

queries.append([header,query])

#########################################


#########################################

#query 4
query = """SELECT DISTINCT game.name AS game
FROM game, award as award1, award as award2
WHERE award1.game = award2.game AND award1.game = game.name AND award1.year = award2.year
AND award1.type = 'STEAM' AND award2.type = 'STEAM'
AND award1.category != award2.category AND award1.winner = '1' AND award2.winner = '1'
AND award1.category IN ('Best VR/AR Game','Student Game Award','Best Music/Sound Design','Best Strategy Game',
'ESports Game of the Year','Best Online Experience','Best VR Game','Best Handheld Game',
'Most Anticipated Game','Best Student Game','Games for Change','Player''s Voice',
'Best Esports Game','Best Multiplayer','Most Anticipated Game 2015','Best Multiplayer Game',
'Best Score/Soundtrack','Best Action Game','Best Fighting Game','Best Score/Music','Best Remaster',
'Best Art Direction','Fresh Indie Game','Global Gaming Citizens','Best Audio Design',
'Best eSports Game','Best Independent Game','Best Shooter','Best Fan Creation','Game of the Year',
'Best Mobile/Handheld Game','Best Sports/Racing Game','Best Game Direction','Best Ongoing Game',
'Best Mobile Game','Games for Impact','Best Family Game','Best Debut Indie Game',
'Best Role Playing Game','Best Action/Adventure Game','Best Community Support',
'Best Narrative','Chinese Fan Game Award')
AND award2.category IN ('Best VR/AR Game','Student Game Award','Best Music/Sound Design','Best Strategy Game',
'ESports Game of the Year','Best Online Experience','Best VR Game','Best Handheld Game',
'Most Anticipated Game','Best Student Game','Games for Change','Player''s Voice',
'Best Esports Game','Best Multiplayer','Most Anticipated Game 2015','Best Multiplayer Game',
'Best Score/Soundtrack','Best Action Game','Best Fighting Game','Best Score/Music','Best Remaster',
'Best Art Direction','Fresh Indie Game','Global Gaming Citizens','Best Audio Design',
'Best eSports Game','Best Independent Game','Best Shooter','Best Fan Creation','Game of the Year',
'Best Mobile/Handheld Game','Best Sports/Racing Game','Best Game Direction','Best Ongoing Game',
'Best Mobile Game','Games for Impact','Best Family Game','Best Debut Indie Game',
'Best Role Playing Game','Best Action/Adventure Game','Best Community Support',
'Best Narrative','Chinese Fan Game Award')"""

header = ["game"]

queries.append([header,query])

#########################################


#########################################

#query 5
query = """SELECT DISTINCT award.nominee AS name, award.year AS year
FROM award
WHERE award.winner = '1'
AND award.category IN ('Developer of the Year','Trending Gamer','Content Creator of the Year','Best Esports Coach',
'Best Esports Host','Best eSports Player','Best Esports Player','Best Performance',
'ESports Player of the Year','Industry Icon Award')"""

header = ["name", "year"]

queries.append([header,query])

#########################################


#########################################

#user defined query
query = """SELECT DISTINCT nominee, game
FROM award
WHERE award.winner = '1'"""

header = ["nominee, game"]

queries.append([header,query])

#########################################


#########################################

#query 7
query = """SELECT DISTINCT developer.name AS developer
FROM game, developer, hasDeveloped, award AS award1, award AS award2
WHERE game.name = hasDeveloped.game AND developer.name = hasDeveloped.name 
AND award1.category = 'Developer of the Year' AND award1.nominee = developer.name
AND award1.winner = '1' AND award2.nominee = award1.nominee AND award2.winner = '1' AND award2.game = game.name
AND award2.category IN ('Best VR/AR Game','Student Game Award','Best Music/Sound Design','Best Strategy Game',
'ESports Game of the Year','Best Online Experience','Best VR Game','Best Handheld Game',
'Most Anticipated Game','Best Student Game','Games for Change','Player''s Voice',
'Best Esports Game','Best Multiplayer','Most Anticipated Game 2015','Best Multiplayer Game',
'Best Score/Soundtrack','Best Action Game','Best Fighting Game','Best Score/Music','Best Remaster',
'Best Art Direction','Fresh Indie Game','Global Gaming Citizens','Best Audio Design',
'Best eSports Game','Best Independent Game','Best Shooter','Best Fan Creation','Game of the Year',
'Best Mobile/Handheld Game','Best Sports/Racing Game','Best Game Direction','Best Ongoing Game',
'Best Mobile Game','Games for Impact','Best Family Game','Best Debut Indie Game',
'Best Role Playing Game','Best Action/Adventure Game','Best Community Support',
'Best Narrative','Chinese Fan Game Award')"""

header = ["developer"]

queries.append([header,query])

#########################################


#########################################

#user defined query
query = """SELECT DISTINCT nominee, game FROM award"""

header = ["nominee, game"]

queries.append([header,query])

#########################################


count = 1

for x in queries:

	#extract the header and the query
	header = x[0]
	query = x[1]
	
	#initialize the cursor and execute the query over the incomplete database
	print("\nexecuting query. . .")
	cursor = connection.cursor()
	cursor.execute(query)
	rows = cursor.fetchall()
	
	result = []
	
	tuples_removed = 0
	
	#initialize the csv file for the results of the queries
	filename = "./queries/query"+str(count)+".csv"
	with open(filename, 'w', encoding="utf-8", newline="") as file:
		csvwriter = csv.writer(file)
	
		#perform the naive evaluation technique
		for row in rows:
			data = []
			for elem in row:
			#identify the variables in each tuple
				if(re.search("^fresh_new_null_representing_a_variable-[0-9]+$", str(elem))==None):
					data.append(elem)
				else:
					data = []
					# print("Null identified, eliminating tuple from results")
					tuples_removed = tuples_removed+1
					break
			
			#if the tuple contains no variables, it is added to the result
			if(len(data)>0):
				result.append(data)
		
		#write in the csv file the proper header followed by the results of the naive evaluation technique
		csvwriter.writerow(header)	
		csvwriter.writerows(result)
	
	if(tuples_removed!=0):
		print("\ttuples discarded because they contained variables : ", tuples_removed)
	
	print("query completed!")
	print("\nqueries executed: ",count)
	count=count+1
	
	print("\n#######################")
	
	cursor.close()








