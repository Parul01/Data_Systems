import os
import sys
import sqlparse
import csv
import copy


from sqlparse.sql import IdentifierList, Identifier


from collections import OrderedDict


def printError(msg):
	print(msg)
	sys.exit()

"""
Two dictionaries will be used initially for storing data
1. storing structure "metaStructure" for every begin_table
2. storing everything "mainDictionary"
	structure 
	mainDictionary{
		"table1":{"attr1":[list of column values],"attr2":[list]...},
		"table2":{... }
		....

	}

"""


headerDictionary = OrderedDict()
metaStructureDictionary = OrderedDict()
mainDictionary = OrderedDict()
mainDictionaryRow = OrderedDict()

METADATAFILE = "metadata.txt"
flag = False

tableName = ""
rowList = []
def readMetaData():
	with open(METADATAFILE) as f:
		fileData = f.readlines()

	for line in fileData:
		if(line.strip() == "<begin_table>"):
			flag = True
		elif(flag):
			tableName = line.strip().lower()
			# below will contain table name  as a key and list (attributes names) as value
			metaStructureDictionary[tableName] =  []
			# mydic = {i : o["name"]}
			tmp = OrderedDict()

			tbl = {tableName : tmp}
			mainDictionary.update(tbl)
			# mainDictionaryRow.update(tbl)
			mainDictionaryRow[tableName] = []
			flag = False
			rowList = []
		elif(line.strip() != "<end_table>"):
			mainDictionary[tableName].update({line.strip().lower(): []})
			metaStructureDictionary[tableName].append(line.strip().lower())
			rowList.append(line.strip().lower())
			headerDictionary[line.strip().lower()] = tableName
		else:
			mainDictionaryRow[tableName].append(copy.deepcopy(rowList))



# fileName = "table1.csv"

def readCSV(fileName):

	with open(fileName+".csv", newline = '\n') as csvFile:
		
		line = csv.reader(csvFile, delimiter =',')

		for row in line:
			# print(" case 2 ")
			row = [int(i) for i in row]
			mainDictionaryRow[fileName.lower()].append(row)
			# print(row)
			# mainDictionaryRow[fileName][row]
			# print("----")
			i = 0
			for x in row:
				mainDictionary[fileName.lower()][metaStructureDictionary[fileName.lower()][i]].append(int(x))
				i += 1
			mainDictionary[fileName.lower()]

readMetaData()
if(flag):
	print("metadata not properly formatted!")
	sys.exit()
# print(metaStructureDictionary)
# print(" ----------- ")
# print(mainDictionary)
for key in metaStructureDictionary:
	readCSV(key)
# readCSV("table1")
# print(mainDictionary)
# print(" ----------- ")
# print(mainDictionaryRow)
# open file one by one get table names from metaStructures

tables = ""
whereQ =[]
selectQ = []
cols = []
isDistinct = 0
distinctCol = ""
groupBy = ""

aggregateQ = ""
orderByQ = ""

headersList = []

# rawQuery = 'SELECT A,b,c,d,e from table1,table2,table3 where a = d;'
# rawQuery = 'select max(A), min(B), D from table1,table2 where A < B and B > 158 group by D order by D DESC;'
# rawQuery = 'select distinct A, D from table1,table2 where A < B and B > 158 group by D order by D ASC;'
# rawQuery = 'select count(*) from table1,table2 where A < B and B > 158;'
rawQuery = sys.argv[1]
# rawQuery = 'select A,B from table1,table2 where A < B and B > 158 group by D;'
# rawQuery = 'SELECT A,B,C,D,E,F,G FROM table1,table2,table3 WHERE A = D;'

rawQuery = rawQuery.strip()
if(rawQuery[-1] != ';'):
	printError("Missing semicolon")

rawQuery =rawQuery.lower()

statements = sqlparse.split(rawQuery)

# ['Select', 'distinct', 'col1, col2', 'from', 'table_name']

errorMsg = "Incorrect query!"
groupFlag = False
tableFlag = False
whereFlag = False
aggregateFlag = False
orderByFlag = False


def parseQuery(queryList):
	global whereFlag,groupFlag,tableFlag,aggregateFlag,orderByFlag,whereQ,isDistinct,cols,selectQ,tables,distinctCol,groupBy,orderByQ
	# print(queryList)
	if(queryList[0] != 'select'):
		print("Incorrect query!")
		sys.exit()

	for i in range(0,len(queryList)):
		# print(i)

		if(queryList[i] == 'select'):
			i += 1
			try:
				while queryList[i] != 'from':
					if(queryList[i] == 'distinct'):
						isDistinct += 1
						if(isDistinct > 1):
							printError(errorMsg)

					if("max" in queryList[i] or "min" in queryList[i] or "count" in queryList[i] or "avg" in queryList[i] or "sum" in queryList[i]):
						aggregateQ = queryList[i]
						aggregateFlag = True

					selectQ.append(queryList[i])
					i += 1
					if i >= len(queryList):
						printError(errorMsg)
			
			except:
				printError(errorMsg)

		if(queryList[i] == "from"):
			try:
				i += 1
				tables = queryList[i]
				continue
			except:
				printError(errorMsg)
			
		if("where" in queryList[i]):
			try:
				 # i += 1
				 # print(" --- entered where --- ")
				 whereFlag = True
				 whereQ = queryList[i]
				 continue
			except:
				printError(errorMsg)
		if(queryList[i] == "group by"):
			try:
				i += 1
				groupBy = queryList[i]
				groupFlag = True
				continue
			except:
				printError(errorMsg)
		
		if(queryList[i].startswith("order by")):
			try:
				orderByFlag = True
				i += 1
				orderByQ = queryList[i]

			except:
				printError(errorMsg)
				

"""	
	listp = ['select', 'table1.C', 'from', 'table1,table2', 'where table1.A<table2.B']
joinTbl = listp[listp.index("from", 0, len(listp))+1]
joinTbl = joinTbl.split(',')
if(len(joinTbl) > 1):
  print("hi")

"""

"""
1. from - execute from (cross product)
2. evaluate where
3. 


"""
output = []
colIndex = OrderedDict()


def joinTables(primary,tbl1,tbl2):
	resultant = []
	# print(tbl1)
	# print(tbl2)
	if primary:
		table1 = mainDictionaryRow[tbl1]
		table2 = mainDictionaryRow[tbl2]
	else:
		table1 = tbl1
		table2 = mainDictionaryRow[tbl2]

	row1 = table1[0]
	row2 = table2[0]
	resultant = []
	# print(row1)
	# print(row2)
	tmpOutput = []
	count = 0
	for i in range(0,len(row1)):
	    # tmpOutput.append(tbl1+"."+row1[i])
	    tmpOutput.append(row1[i])
	    colIndex[row1[i]] = count
	    count += 1
	for i in range(0,len(row2)):
	    # tmpOutput.append(tbl2+"."+row2[i])
	    tmpOutput.append(row2[i])
	    colIndex[row2[i]] = count
	    count += 1

	resultant.append(tmpOutput)
	for i in range(1,len(table1)):
	    tmp = table1[i]
	    for j in range(1,len(table2)):
	    	resultant.append(tmp + table2[j])
	      
	return resultant


def operate(op,val1,val2):

	if op.strip() == '=':
		if int(val1) == int(val2):
			return True
		else:
			return False


	if op.strip() == '>':
		if int(val1) > int(val2):
			return True
		else:
			return False

	if op.strip() == '<':
		if int(val1) < int(val2):
			return True
		else:
			return False

	if op.strip() == '>=':
		if int(val1) >= int(val2):
			return True
		else:
			return False

	if op.strip() == '<=':
		if int(val1) <= int(val2):
			return True
		else:
			return False
	else:
		printError("Incorrect SQL query")
		sys.exit()


def executeSelect(output,selectQ):
	global colIndex, headersList

	res = output[0]
	output = output[1:]
	resUpd = []

	outputList = []

	if (aggregateFlag and 'count(*)' not in selectQ):
		# print("case -- 1")
		outList = []
		if(groupFlag):

			# print("case -- 2")
			# print(groupBy)
			outputList = []
			selectQ = selectQ[0].split(',')
			# print(selectQ)
			colList = [x.strip() for x in selectQ]
			# print("col list")
			# print(colList)


			isFirst = True
			for c in colList:
				if (c.strip().startswith("max") or c.strip().startswith("min") or c.strip().startswith("avg") or c.strip().startswith("count") or c.strip().startswith("avg")):
					# aggrList[c.split("(")[0]] = c.split("(")[1][0:-1]
					aggr = c.split("(")[0]
					colname = c.split("(")[1][0:-1]
					resUpd.append(colname)
					# 		get list of that colum
					# elavluate max(A)
					maintainValue = output[0][colIndex[groupBy]]
					# print("maintain value")
					# print(maintainValue)
					if isFirst:
						# print("  -- 1  --")
						tmpList = []
						for orow in output:
							for ocol in range(len(orow)):
								if colIndex[colname] == ocol:
									if maintainValue != orow[colIndex[groupBy]]:
										tmp = eval(aggr)(tmpList)
										ab = list()
										ab.append(tmp)
										outList.append(ab)
										maintainValue = orow[colIndex[groupBy]]
										tmpList = []
										tmpList.append(orow[ocol])
									else:
										tmpList.append(orow[ocol])

						tmp = eval(aggr)(tmpList)
						ab = list()
						ab.append(tmp)
						outList.append(ab)

						# print(outList)
						isFirst = False
					else:
						# print("  -- 2  --")
						if isFirst:

							maintainValue = output[0][colIndex[groupBy]]
							tmpList = []
							for orow in output:
								for ocol in range(len(orow)):
									if colIndex[colname] == ocol:
										if maintainValue != orow[colIndex[groupBy]]:
											tmp = eval(aggr)(tmpList)
											ab = []
											ab.append(tmp)
											outList.append(ab)
											maintainValue = orow[colIndex[groupBy]]
											tmpList = []
											tmpList.append(orow[ocol])
										else:
											tmpList.append(orow[ocol])
							# count += 1
							tmp = eval(aggr)(tmpList)
							ab = []
							ab.append(tmp)
							outList.append(ab)
							isFirst = False
						else:
							count = 0
							maintainValue = output[0][colIndex[groupBy]]
							tmpList = []
							for orow in output:
								for ocol in range(len(orow)):
									if colIndex[colname] == ocol:
										if maintainValue != orow[colIndex[groupBy]]:
											tmp = eval(aggr)(tmpList)
											outList[count].append(tmp)
											maintainValue = orow[colIndex[groupBy]]
											tmpList = []
											count += 1
											tmpList.append(orow[ocol])
										else:
											tmpList.append(orow[ocol])
							# count += 1
							tmp = eval(aggr)(tmpList)
							outList[count].append(tmp)
						# print(" 2 outList")
						# print(outList)

				else:
					# print("  -- 3  --")
					resUpd.append(groupBy)
					if(c.strip() != groupBy):
						printError("Invalid")
					else:
						if isFirst:
							maintainValue = output[0][colIndex[groupBy]]

							for orow in output:
								for ocol in range(len(orow)):
									if colIndex[groupBy] == ocol:
										if maintainValue != orow[colIndex[groupBy]]:
											# tmp = eval(aggr)(tmpList)
											ab = []
											ab.append(maintainValue)
											outList.append(ab)
											maintainValue = orow[colIndex[groupBy]]
											# tmpList = []


							# print(outList)
							ab = []
							ab.append(maintainValue)
							outList.append(ab)

							isFirst = False
						else:
			# 			just insert value of d
							maintainValue = output[0][colIndex[groupBy]]
							count = 0
							for orow in output:
								for ocol in range(len(orow)):
									if colIndex[groupBy] == ocol:
										if maintainValue != orow[colIndex[groupBy]]:
											# tmp = eval(aggr)(tmpList)
											outList[count].append(maintainValue)
											maintainValue = orow[colIndex[groupBy]]
											# tmpList = []
											count += 1

							# print(outList)
							outList[count].append(maintainValue)
			# print(outList)
			headersList = copy.deepcopy(colList)
			outputList = copy.deepcopy(outList)
			outputList.insert(0,resUpd)
			return outputList
		else:
			# print("case -- 3")
			colList = [x.strip() for x in selectQ[0].split(',')]


			outList = []
			for c in colList:
				if(c.startswith("max") or c.startswith("min") or c.startswith("avg") or c.startswith("count")):

					aggr = c.split("(")[0]
					colname = c.split("(")[1][0:-1]
			# 		get list of that colum
					tmpList = []
					for orow in output:
						for ocol in range(len(orow)):
							if colIndex[colname] == ocol:
								tmpList.append(orow[ocol])

					tmp = (eval(aggr)(tmpList))
					outList.append(tmp)
			headersList = copy.deepcopy(colList)
			outputList.append(colList)
			outputList.append(outList)
			return outputList

	elif '*' in selectQ:

		# print(selectQ)
		# if(aggregateFlag):
		# 	if aggregateQ
		#
		# else:
		# 	print("case -- 4")
		outputList = copy.deepcopy(output)
		outputList.insert(0, res)
		return outputList

	elif 'count(*)' in selectQ:
	# 	count no of rows
		ll = len(output)
		ad = []
		ad.append(ll)
		headersList = ['count(*)']
		outputList.insert(0,['count(*)'])
		outputList.append(ad)
		return outputList
	else:

		# print("case -- 5")
		# print(selectQ)
		#  return just those columns
		if "distinct" in selectQ:
			# print("case -- 6")

			selectQ = selectQ[1:]
			selectQ = selectQ[0].split(',')

			listp = [colIndex[c.strip()] for c in selectQ]
			for orow in output:
				rowList = []
				for ocol in range(len(orow)):
					if ocol in listp:
						rowList.append(orow[ocol])
				if rowList not in outputList:
					outputList.append(copy.deepcopy(rowList))

			ll = []
			ll = copy.deepcopy(res)
			for x in res:
				if colIndex[x.strip()] not in listp:
					ll.remove(x)

			outputList.insert(0,ll)

		# print('---')
		# if isDistinct:
		# 	print("in distinct case :")
		# 	print(selectQ)
		# 	selectQ = selectQ[1:]
		# 	for c in selectQ:
		# 		colIndex[c]

			# select only distinct columns
		else:
			# print("case -- 7")
			# print(selectQ)
			selectQ = selectQ[0].split(',')
			try:
				listp = [colIndex[c.strip()] for c in selectQ]
			except:
				sys.exit("Invalid query!")

			for orow in output:
				rowList = []
				for ocol in range(len(orow)):
					if ocol in listp:
						rowList.append(orow[ocol])

				outputList.append(copy.deepcopy(rowList))
			# print("output list")
			# print(outputList)

			ll = []
			ll = copy.deepcopy(res)
			for x in res:
				if colIndex[x.strip()] not in listp:
					ll.remove(x)

			outputList.insert(0, ll)
	# 		return just those columns mentioned in selectQ
	# 		selectColumns =
			# for orow in range(output):
			# 	for ocol in range(orow):
			# 		if colIndex[selectQ] == ocol
			#
			#
			# for i in range(selectQ):
			# 	# get column index
			# 	colmn = colIndex[selectQ[i]]
			# 	if i == colIndex[selectQ[i]]:
			# 		outputList.append(output[i])

	return outputList



def executeGroupBy(outputList,col):
	global colIndex
	# print("colIndex")
	# print(colIndex)
	output = []

	res = outputList[0]
	outputList = outputList[1:]

	index = colIndex[col]
	mapColumn = OrderedDict()

	for i in range(len(outputList)):
		if(outputList[i][index] not in mapColumn):
			mapColumn[outputList[i][index]] = [i]
		else:
			mapColumn[outputList[i][index]].append(i)


	for key, value in mapColumn.items():
		ll = value
		for x in ll:
			output.append(outputList[x])

	# return (output.insert(0,res))
	output.insert(0,res)
	return output


def executeWhere(connector,part1,part2,output,flag):
	global colIndex
	if(flag):

		res = output[0]
		output = output[1:]
		
		outputUpd = []

		opnd1 = part1[0].strip()
		opnd2 = part1[2].strip()

		opnd3 = part2[0].strip()
		opnd4 = part2[2].strip()
		# print(" ------ ")
		# print(opnd4)
		# print(opnd4.isdigit())
		# print(" ------ ")

		op1 = part1[1]
		op2 = part2[1]

		
		if(opnd2.isdigit() and opnd4.isdigit()):
			# print("case 1")
			for outp in output:
				if connector == 'and':
					if(operate(op1,outp[colIndex[opnd1]],opnd2) and operate(op2,outp[colIndex[opnd3]],opnd4)):
						outputUpd.append(outp)
				elif connector == 'or':
					if(operate(op1,outp[colIndex[opnd1]],opnd2) or operate(op2,outp[colIndex[opnd3]],opnd4)):
						outputUpd.append(outp)

		if(opnd2.isdigit() and (not(opnd4.isdigit()))):
			# print("case 2")
			for outp in output:
				if connector == 'and':
					if(operate(op1,outp[colIndex[opnd1]],opnd2) and operate(op2,outp[colIndex[opnd3]],outp[colIndex[opnd4]])):
						outputUpd.append(outp)
				elif connector == 'or':
					if(operate(op1,outp[colIndex[opnd1]],opnd2) or operate(op2,outp[colIndex[opnd3]],outp[colIndex[opnd4]])):
						outputUpd.append(outp)

		if((not(opnd2.isdigit())) and (opnd4.isdigit())):

			# print("case 3")
			for outp in output:
				if connector == 'and':
					if(operate(op1,outp[colIndex[opnd1]],outp[colIndex[opnd2]]) and operate(op2,outp[colIndex[opnd3]],opnd4)):
						outputUpd.append(outp)
				elif connector == 'or':
					if(operate(op1,outp[colIndex[opnd1]],outp[colIndex[opnd2]]) or operate(op2,outp[colIndex[opnd3]],opnd4)):
						outputUpd.append(outp)

		if((not(opnd2.isdigit())) and (not(opnd4.isdigit()))):

			# print("case 4")

			try:

				for outp in output:
					if connector.strip() == 'and':

						if(operate(op1,outp[colIndex[opnd1.strip()]],outp[colIndex[opnd2.strip()]]) and operate(op2,outp[colIndex[opnd3.strip()]],outp[colIndex[opnd4.strip()]])):
							outputUpd.append(outp)
					elif connector.strip() == 'or':
						if(operate(op1,outp[colIndex[opnd1.strip()]],outp[colIndex[opnd2.strip()]]) or operate(op2,outp[colIndex[opnd3.strip()]],outp[colIndex[opnd4.strip()]])):
							outputUpd.append(outp)
			except:
				sys.exit("Invalid Query!")

		# outputUpd = res + outputUpd
		outputUpd.insert(0,res)
		return outputUpd

	else:
		res = output[0]
		output = output[1:]
		
		outputUpd = []

		opnd1 = part1[0]
		opnd2 = part1[2]

		op1 = part1[1]
		
		if(opnd2.isdigit()):
			for outp in output:
				if(operate(op1,outp[colIndex[opnd1]],opnd2)):
						outputUpd.append(outp)
				
		if((not(opnd2.isdigit()))):
			for outp in output:

				if(operate(op1,outp[colIndex[opnd1]],outp[colIndex[opnd2]])):
					outputUpd.append(outp)

		outputUpd.insert(0,res)
		return outputUpd

def executeOrderBy(output,orderByQ):

	res = output[0]
	output = output[1:]
	outputList = []
	orderByQ = orderByQ.strip()
	# print(orderByQ)
	orderByQ = orderByQ.split(" ")
	if len(orderByQ) > 1:
		orderByCol = orderByQ[0].strip()
		orderByCon = orderByQ[1].strip()
	else:
		orderByCol = orderByQ[0].strip()
		orderByCon = 'asc'


	index = res.index(orderByCol,0,len(res))

	if(orderByCon.lower() == 'asc'):
		output = sorted(output, key=lambda x: x[index])
	elif(orderByCon.lower() == 'desc'):
		output = sorted(output, key=lambda x: x[index], reverse=True)
	else:
		sys.exit("Invalid query!")
	outputList = copy.deepcopy(output)
	outputList.insert(0,res)
	return outputList

def processQuery(tokensList):
	global whereFlag,groupFlag,tableFlag,aggregateFlag,orderByFlag,whereQ,isDistinct,cols,selectQ,tables,distinctCol,groupBy,colIndex
	# tables = ""
	# whereQ =[]
	# selectQ = ""
	# cols = []
	# isDistinct = 0
	# distinctCol = ""
	# groupBy = ""
	# aggregateFlag = False
	# orderByFlag = False
	# aggregateQ = ""
	# orderByQ = ""
	# groupFlag = False
	# tableFlag = False

	parseQuery(tokensList)
	# print(" --- checking flag value ------ "+str(whereFlag))
	# from
	joinTbl = tokensList.index("from",0,len(tokensList))
	joinTbl = tokensList[joinTbl+1].split(',')
	primary = True
	try:
		if(len(joinTbl) > 1):
			# print(joinTbl)
			tmpOutput = joinTbl[0]
			for i in range(0,len(joinTbl)-1):
				tmpOutput = joinTables(primary,tmpOutput,joinTbl[i+1])
				primary = False
			output = copy.deepcopy(tmpOutput)
			# print(" --- ")
			# display(output)
		else:
			# print(joinTbl)
			output = mainDictionaryRow[joinTbl[0]]
			# display(output)
			count = 0
			for col in output[0]:
				colIndex[col] = count
				count += 1
	except:
		sys.exit("Invalid query!")

	# display(output)
	if (whereFlag):
		# ['where', 'A', '<', 'B', 'and', 'B', '>', '158']

		# print(" --- entered ----")
		# compute where
		# considering spaces are there
		# there'll be maximum of 1 and/or
		connector = ""
		whereQ = whereQ.split(' ')
		whereQ = whereQ[1:]
		# print(whereQ)
		if("and" in whereQ):
			connector = "and"
		elif("or" in whereQ):
			connector = "or"

		if(connector != ""):
			idx = whereQ.index(connector)
			part1 = whereQ[0:idx]
			part2 = whereQ[idx+1:]
			output = executeWhere(connector,part1,part2,output,True)
		else:
			output = executeWhere(connector,whereQ,"",output,False)


	if(groupFlag):
		# groupByCol = groupBy
		# //call funct
		output = executeGroupBy(output,groupBy)
		# print("group by")
		# display(output)
		# print("group by")

		# print(part1)
		# print(part2)

		# print(connector)
		# print(whereQ)
		# if(whereQ[6].isdigit()):

		# 	print("-- digit --")

		# for i in whereQ:
		# 	make_tokens = i.split('')  # see if there'll be space
	# print("selectQ")
	# print(selectQ)
	output = executeSelect(output,selectQ)

	if(orderByFlag):
		output = executeOrderBy(output,orderByQ)
	return output

# def processHeader(output,selectQ):
# 	output = output[1:]
# 	selectQ = selectQ.split(',')

# get list -----------------------------------------------------------------------------------------------------
def tokenize(stmnt):
	tokensList = []
	parsedQuery = sqlparse.parse(stmnt)[0].tokens
	idObject = IdentifierList(parsedQuery).get_identifiers()

	for x in idObject:
		tokensList.append(str(x))
	return tokensList


def display(outputList):

	for ot in outputList:
		for co in range(len(ot)):
			if(co != len(ot)-1):
				print(ot[co], end=',')
			else:
				print(ot[co])


def generateHeaders(selectQ,output):
	# print(headerDictionary)
	try:

		if(aggregateFlag):
			output = output[1:]
			output.insert(0,headersList)
			return output
		else:
			# selectQ = selectQ[1:]
			# print(selectQ)
			if(isDistinct):
				selectQ = selectQ[1:]


			selectQ = selectQ[0].split(',')

			if '*' in selectQ:
				tl = []
				for key in colIndex:
					tl.append(key)

				hh = [headerDictionary[x.strip()] + "." + x.strip() for x in tl]
				output = output[1:]
				output.insert(0, hh)
				return output

			hh = [headerDictionary[x.strip()]+"."+x.strip() for x in selectQ]
			output = output[1:]
			output.insert(0,hh)
			return output
	except:
		sys.exit("Invalid query")


for stmnt in statements:
	# print(stmnt)
	tokensList = tokenize(stmnt)
	# print(tokensList)
	
	if(tokensList[-1].startswith("where")):
		# print("Case 1")
		tokensList[-1] = tokensList[-1][:-1]

	elif(tokensList[-1] != ';'):
		print("missing semicolon, please format query properly!")
		sys.exit()
	else:
		tokensList = tokensList[:-1]

	# print(tokensList)

	# print(tokensList)
	try:
		output = processQuery(tokensList)
	except:
		sys.exit("Invalid query!")
	# print("after where")
	# res = processHeader(output,selectQ)
	output = generateHeaders(selectQ,output)
	display(output)
	# process query


