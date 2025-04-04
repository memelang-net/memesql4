#!/usr/bin/env python3

import sys, os, re, glob, db
from db import DB

DB['table_seqn']='seqn'
DB['table_node']='node'
DB['table_numb']='numb'
DB['table_name']='name'

###############################################################################
#                           CONSTANTS & GLOBALS
###############################################################################

# Global dictionary to cache key->id mappings
KEYS = {}
GID = 999 # Default graph ID

I = {
	' '   : 1,
	'='   : 2,
	'['   : 3,
	']'   : 4,
	'>>'  : 5,
	';'   : 6,

	'="'  : 10,
	'=='  : 13,
	'>'   : 14,
	'<'   : 15,
	'>='  : 16,
	'<='  : 17,
	'!='  : 18,

	'nam' : 2**9 + 0,
	'key' : 2**9 + 1,
	'tit' : 2**9 + 2,

	'act' : 2**9 + 3,
	'get' : 2**9 + 4,
	'cnt' : 2**9 + 5,
	'put' : 2**9 + 6,
	'mod' : 2**9 + 7,
	'idx' : 2**9 + 8,
	'del' : 2**9 + 9,
	'wip' : 2**9 + 10,

	'cor' : 2**29
}

# Lazy population for now
K = {value: key for key, value in I.items()}

RID, BID, AID, AMT, ALP = 'rid', 'bid', 'aid', 'amt', 'alp'
TBL = {RID: DB['table_node'], AID: DB['table_node'], AMT: DB['table_numb'], ALP: DB['table_name']}

FUNC, CLMN, TIER, OBEG, OEND = 0, 1, 2, 3, 4
TERM, TAND, TFWD, TREV, TIMP, TEND = 0, 1, 2, 3, 4, 5
XO, XV, YO, YV, TLEN = 0, 1, 2, 3, 4
VALS, OPRS = (XV, YV), (XO, YO)

OPR = { # Each operator and its meaning
	None     : [None, None, None, None, None],
	I[' ']   : [XO, RID, TAND, False, False],
	I['>>']  : [XO, RID, TIMP, False, False],
	I[';']   : [XO, RID, TEND,  "\n", False],

	I['[']   : [XO, RID, TFWD, False, False],
	I[']']   : [XO, RID, TREV, False, False],

	I['=']   : [YO, AID, TERM, False, False],

	I['="'] : [YO, ALP, TERM, False, '"'],
	I['==']  : [YO, AMT, TERM, '=', False],
	I['>']   : [YO, AMT, TERM, False, False],
	I['<']   : [YO, AMT, TERM, False, False],
	I['>=']  : [YO, AMT, TERM, False, False],
	I['<=']  : [YO, AMT, TERM, False, False],
	I['!=']  : [YO, AMT, TERM, False, False],
}

# For decode()
INCOMPLETE, SEMICOMPLETE, COMPLETE = 1, 2, 3
OPRSTR = {
	'!'   : [INCOMPLETE, False],
	'>'   : [SEMICOMPLETE, I['>']],
	'<'   : [SEMICOMPLETE, I['<']],
	'='   : [SEMICOMPLETE, I['=']],
	'=='  : [COMPLETE, I['==']],
	'!='  : [COMPLETE, I['!=']],
	'>='  : [COMPLETE, I['>=']],
	'<='  : [COMPLETE, I['<=']],
	';'   : [COMPLETE, I[';']],
	' '   : [COMPLETE, I[' ']],
	'>>'  : [COMPLETE, I['>>']],
	'['   : [COMPLETE, I['[']],
	']'   : [COMPLETE, I[']']],
}


###############################################################################
#                       MEMELANG STRINGING PROCESSING
###############################################################################

# Input: Memelang string as "operator1operand1operator2operand2"
# Output: memetoks as [[[XO, XV, YO, YV]], ...]
def decode(memestr: str) -> list:

	memestr = re.sub(r'\s*//.*$', '', memestr, flags=re.MULTILINE).strip() # Remove comments
	if len(memestr) == 0: raise Exception('api=err fld=qry msg=empty\n')

	memetoks, expressions = [], []
	terms = [None, None, None, None]
	operator = None

	parts = re.split(r'(?<!\\)"', ';'+memestr)
	for p, part in enumerate(parts):

		# Quote
		if p%2==1:
			terms[YO], terms[YV] = I['="'], part
			continue

		part = re.sub(r'\s*\\\s*', ' ', part)			# Backslash is same line
		part = re.sub(r'[;\n]+', ';', part)				# Newlines are semicolons
		part = re.sub(r'\s+', ' ', part)				# Remove multiple spaces
		part = re.sub(r'\s+(?=[\]\[&])', '', part)		# Remove spaces before operators
		part = re.sub(r'\s*;+\s*', ';', part)			# Remove multiple semicolons
		part = re.sub(r';+$', '', part)					# Remove ending semicolon

		# Split by operator characters
		strtoks = re.split(r'([\]\[;!><=\s])', part)
		tlen = len(strtoks)
		t = 0
		while t<tlen:
			strtok=strtoks[t]

			# Skip empty
			if len(strtok)==0: pass

			# Operator
			elif OPRSTR.get(strtok):

				# We might want to rejoin two sequential operator characters
				# Such as > and =
				completeness, operator = OPRSTR[strtok]
				if completeness!=COMPLETE:
					for n in (1,2):
						if t<tlen-n and len(strtoks[t+n]):
							if OPRSTR.get(strtok+strtoks[t+n]):
								completeness, operator = OPRSTR[strtok+strtoks[t+n]]
								t+=n
							break
					if completeness==INCOMPLETE: raise Exception(f'api=err fld=strtok msg=invalid val="{strtok}"')

				if OPR[operator][TIER] >= TAND:
					if terms[XV] or terms[YV] or terms[YO]: expressions.append(terms)
					terms = [operator, None, None, None]
					if OPR[operator][TIER] >= TFWD:
						if OPR[operator][TIER] >= TIMP:
							if expressions: memetoks.append(expressions)
							expressions = []
				else: terms[OPR[operator][FUNC]]=operator

			# Key/Integer/Decimal
			else:
				if operator is None: raise Exception(f'Sequence error at {strtok}')
				if re.search(r'[^a-zA-Z0-9\.\-]', strtok): raise Exception(f"Unexpected '{strtok}' in {memestr}")

				elif OPR[operator][FUNC]==XO:
					if strtok.isdigit(): strtok=int(strtok)

				elif OPR[operator][FUNC]==YO:
					# R=a1234
					if re.fullmatch(r'a[0-9]+', strtok):
						strtok=int(strtok[1:])

					# R=1234
					elif (re.search(r'[0-9]', strtok) and not re.search(r'[a-zA-Z]', strtok)):
						strtok=float(strtok)
						if operator==I['=']:
							operator=I['==']
							terms[YO]=operator

				terms[OPR[operator][FUNC]+1]=strtok

			t+=1

	if terms[XV] or terms[YV] or terms[YO]: expressions.append(terms)
	if expressions: memetoks.append(expressions)

	normalize(memetoks)

	return memetoks

# Input: memetoks as [[[XO, XV, YO, YV]], ...]
# Output: Memelang string "operator1operand1operator2operand2"
def encode(memetoks: list) -> str:
	memestr = ''
	for s, expressions in enumerate(memetoks):
		for e, terms in enumerate(expressions):
			for t in OPRS:
				if terms[t] is None: continue
				memestr += K[terms[t]] if OPR[terms[t]][OBEG] is False else OPR[terms[t]][OBEG]
				if terms[t+1] is not None: memestr += str(terms[t+1])
				if OPR[terms[t]][OEND]: memestr += OPR[terms[t]][OEND]
	return memestr


# [[[XO, XV, YO, YV]], ...]
def normalize(memetoks: list[list]):
	for s, expressions in enumerate(memetoks):
		for e, terms in enumerate(expressions):
			if len(terms)!=TLEN: raise Exception(f"Term count error for at {s}:{e}")

			# Clean all
			for t in range(TLEN):
				if isinstance(terms[t], str):
					terms[t]=terms[t].strip()
					if terms[t].isdigit(): terms[t]=int(terms[t])
				elif isinstance(terms[t], bool): terms[t]=None

				# Operators
				if t in OPRS:
					if isinstance(terms[t], str):
						if not I[terms[t]] or not OPR.get(I[terms[t]]): raise Exception(f"Operator error for {terms[t]} at {s}:{e}:{t}")
						terms[t]=I[terms[t]]
					elif terms[t] is not None and not OPR.get(terms[t]): raise Exception(f"Operator error for {terms[t]} at {s}:{e}:{t}")

			# Numeric value
			if OPR[terms[YO]][CLMN]==AMT:
				if isinstance(terms[YV], int): terms[YV]=float(terms[YV])
				elif isinstance(terms[YV], str):
					try: terms[YV] = float(terms[YV])
					except ValueError: raise Exception(f"String operator error for {terms[YO]} {terms[YV]} at {s}:{e}")

			memetoks[s][e]=terms



###############################################################################
#                           KEY <-> ID CONVERSIONS
###############################################################################

# Input list of key strings ['GeorgeWashington', 'JohnAdams']
# Load key->aids in I and K caches
# I['JohnAdams']=123
# K[123]='JohnAdams'
def identify(memetoks: list[list], gid: int = GID):
	allaids=I
	if not KEYS.get(gid): KEYS[gid]={}
	for q, a in KEYS[gid].items(): allaids.setdefault(q.lower(), a)

	lookups={}

	for s, expressions in enumerate(memetoks):
		for e, terms in enumerate(expressions):
			for t in VALS:
				if isinstance(terms[t], str) and not allaids.get(terms[t]): lookups[terms[t].lower()]=1

	if lookups:
		rows=db.selectin({f'LOWER({ALP})':lookups.keys(), 'rid':[I['key']], 'gid':[gid]}, DB['table_name'])
		for row in rows: 
			KEYS[int(row[0])][row[3]] = int(row[1])
			allaids.setdefault(row[3].lower(), int(row[1]))

	for s, expressions in enumerate(memetoks):
		for e, terms in enumerate(expressions):
			for t in VALS:
				if isinstance(terms[t], str):
					iid = allaids.get(terms[t].lower(),0)
					if iid == 0: raise Exception(f"Unknown identifier \"{terms[t]}\" in {terms}")
					memetoks[s][e][t]=iid


def keyify(memetoks: list[list], gid: int = GID) -> list:
	allstrs=K
	if not KEYS.get(gid): KEYS[gid]={}
	for q, a in KEYS[gid].items(): allstrs.setdefault(a, q)

	lookups={}

	for s, expressions in enumerate(memetoks):
		for e, terms in enumerate(expressions):
			for t in VALS:
				if isinstance(terms[t], int) and not allstrs.get(terms[t]): lookups[terms[t]]=1

	if lookups:
		rows=db.selectin({'bid':lookups.keys(), 'rid':[I['key']], 'gid':[gid]}, DB['table_name'])
		for row in rows: KEYS[int(row[0])][row[3]] = int(row[1])
		for q, a in KEYS[gid].items(): allstrs.setdefault(a, q)

	for s, expressions in enumerate(memetoks):
		for e, terms in enumerate(expressions):
			for t in VALS:
				if isinstance(terms[t], int): memetoks[s][e][t]=allstrs[terms[t]]


# Run decode() and identify()
def idecode(memestr: str, gid: int = GID) -> list:
	memetoks = decode(memestr)
	identify(memetoks, gid)
	return memetoks


# Run keyify() and encode()
def keyencode(memetoks: list[list], gid: int = GID) -> str:
	keyify(memetoks, gid)
	return encode(memetoks)


###############################################################################
#                         MEMELANG -> SQL QUERIES
###############################################################################

# Input: memetoks
# Output: "SELECT x FROM y WHERE z", [param1, param2, ...]
def selectify(expressions: list[list], gid: int = GID) -> tuple[str, list]:

	n       = 0
	bb      = [n]
	aa      = [n]
	gid     = str(int(gid))
	tbl     = DB['table_node']
	where 	= f"n{n}.gid={gid}"
	groupby = f"n{n}.bid"
	join    = ''
	select  = "';'"
	params 	= []
	acol    = None
	aacol   = None

	for terms in expressions:
		tier = OPR[terms[XO]][TIER]
		tbl = DB['table_node']
		acol = AID

		if terms[YO]:
			acol = OPR[terms[YO]][CLMN]
			tbl = TBL[acol]

		if tier == TEND:
			join 	+= f' FROM {tbl} n{n}'
			aacol   = acol

		elif tier == TAND:
			n+=1
			bb.append(n)
			join += f" LEFT JOIN {tbl} n{bb[-1]} ON n{aa[-1]}.bid=n{bb[-1]}.bid"
			if acol == AID and aacol == AID:
				join += f" AND (n{aa[-1]}.aid!=n{bb[-1]}.aid OR n{aa[-1]}.rid!=n{bb[-1]}.rid)"

		elif tier == TFWD:
			n+=1
			aa.append(n)
			bb.append(n)
			select	+= f", string_agg(DISTINCT ' ' || n{bb[-2]}.rid || '[' || n{bb[-1]}.rid, '')"
			join	+= f" JOIN {tbl} n{bb[-1]} ON n{bb[-2]}.aid=n{bb[-1]}.aid AND n{bb[-1]}.gid={gid} AND n{bb[-2]}.bid!=n{bb[-1]}.bid"
			groupby += f", n{bb[-1]}.aid, n{bb[-1]}.rid, n{bb[-1]}.bid"
			aacol    = acol

		elif tier == TREV:
			select	+= ", ']'"
			aa.pop()
			continue

		if terms[XV] is not None:
			where += f" AND n{n}.rid=%s"
			params.append(terms[XV])

		if acol == AID:
			select 	+= f", string_agg(DISTINCT ' ' || n{n}.rid || '=a' || n{n}.aid, '')"
			if terms[YV] is not None:
				where += f" AND n{n}.aid=%s"
				params.append(terms[YV])

		elif acol == ALP:
			select 	+= f", string_agg(DISTINCT ' ' || n{n}.rid || '=\"' || n{n}.alp || '\"', '')"
			if terms[YV] is not None:
				where += f" AND LOWER(n{n}.alp) LIKE %s"
				params.append(terms[YV].lower())

		elif acol == AMT: 
			select 	+= f", string_agg(DISTINCT ' ' || n{n}.rid || '==' || n{n}.amt, '')"
			if terms[YV] is not None:
				cpr = '=' if terms[YO] == I['=='] else K[terms[YO]]
				where += f" AND n{n}.amt{cpr}%s"
				params.append(terms[YV])

		if aacol!=AMT and terms[XV] is None and terms[YV] is None:
			n+=1
			select 	+= f", string_agg(DISTINCT ' ' || n{n}.rid || '==' || n{n}.amt, '')"
			join += f" LEFT JOIN {DB['table_numb']} n{n} ON n{aa[-1]}.bid=n{n}.bid"

	return f"SELECT CONCAT({select}) AS raq {join} WHERE {where} GROUP BY {groupby}", params


# Input: Memelang query string
# Output: SQL query string
def querify(memetoks: list[list], gid: int = GID) -> tuple[str, list]:
	selects, params = [], []
	for s, expressions in enumerate(memetoks):
		qry_select, qry_params = selectify(expressions, gid)
		selects.append(qry_select)
		params.extend(qry_params)
	return ' UNION '.join(selects), params


# Input: Memelang string
# Saves to DB
# Output: Memelang string
def put (memestr: str, gid: int) -> str:
	
	if not gid: raise Exception('put gid')
	if gid not in KEYS: KEYS[gid]={}

	memetoks = decode(memestr)

	rows = {DB['table_node']:[], DB['table_name']:[], DB['table_numb']:[]}
	params = {DB['table_node']:[], DB['table_name']:[], DB['table_numb']:[]}

	# NEW KEY NAMES

	newkeys = {}
	l2u = {}

	for s, expressions in enumerate(memetoks):
		for e, terms in enumerate(expressions):
			for t in VALS:
				if terms[t] is None: raise Exception(f'Invalid null {terms}')
				if isinstance(terms[t], str):
					if iid := KEYS[gid].get(terms[t]): memetoks[s][e][t]=iid
					elif re.search(r'[^a-zA-Z0-9]', terms[t]): raise Exception(f'Invalid key {terms[t]} in {terms}')
					else: 
						alp = db.slugify(terms[t])
						alpl = alp.lower()
						if not newkeys.get(alpl):
							newkeys[alpl] = 0
							l2u[alpl] = alp

	if newkeys:
		# Unique check keys
		krows=db.selectin({'gid':[gid], 'rid':[I['key']], f'LOWER({ALP})':newkeys.keys()}, DB['table_name'])
		for row in krows:
			alp=row[3]
			alpl = alp.lower()
			if newkeys.get(alpl):
				if int(row[1]) == int(newkeys[alpl]) or newkeys[alpl] is None: newkeys.pop(alpl, 0)
				else: raise Exception(f"Duplicate key {alp} for new {newkeys[alpl]} and old {row[1]}")

		# Write new keys
		for alpl in newkeys:
			alp = l2u[alpl]

			if re.search(r'[^a-zA-Z0-9]', alpl) or not re.search(r'[a-zA-Z]', alpl):
				raise Exception(f'Invalid key at {alpl}')

			aid = newkeys[alpl]
			if not aid: aid = db.seqinc(DB['table_seqn'])
			elif aid<=I['cor']: raise Exception(f'Invalid id number {aid}')

			KEYS[gid][alp]=aid
			rows[DB['table_name']].append("(%s,%s,%s,%s)")
			params[DB['table_name']].extend([gid, aid, I['key'], alp])

		# Swap missing keys for new IDs
		identify(memetoks, gid)
	
	# NEW MEMES
	for s, expressions in enumerate(memetoks):
		bid = db.seqinc(DB['table_seqn'])
		for e, terms in enumerate(expressions):
			col = OPR[terms[YO]][CLMN]
			if col == AID: tbl = DB['table_node']
			elif col == AMT: tbl = DB['table_numb']
			elif col == ALP: tbl = DB['table_name']
			else: raise Exception('put col')
			params[tbl].extend([gid, bid, terms[XV], terms[YV]])
			rows[tbl].append('(%s,%s,%s,%s)')

	sqls=[]
	for tbl in params:
		if params[tbl]: sqls.append(f"INSERT INTO {tbl} VALUES " + ','.join(rows[tbl]) + " ON CONFLICT DO NOTHING")
		else: sqls.append(None)

	db.inserts(sqls, params.values())

	keyencode(memetoks)
	return memetoks


# Input: Memelang query string
# Output: Memelang results string
def query(memestr: str = None, gid: int = GID) -> str:
	memetoks = decode(memestr);

	if not memetoks: return False

	action = 'get'
	for terms in memetoks[0]:
		if terms[XV] == 'act':
			if isinstance(terms[YV], str):
				if not I.get(terms[YV].lower()): raise Exception('invalid act=')
				action=terms[YV]
				memetoks.pop(0)
				break

	if action == 'put': return put(memetoks, gid)
	else:
		identify(memetoks)
		if action == 'get': return keyencode(get(memetoks, gid), gid)
		elif action == 'cnt': return 'act=cnt amt=' + str(count(memetoks, gid))
		elif action == 'wip': return 'act=wip amt=' + str(wipe(memetoks, gid))
		else: raise Exception('query action error')


def get(memetoks: list, gid: int = GID) -> str:

	sql, params = querify(memetoks, gid)
	res = db.select(sql, params)

	if not res: return ''

	responsestr=''
	for row in res: responsestr+=row[0]

	return decode(responsestr)


# Input: Memelang query string
# Output: Integer count of resulting memes
def count(memetoks: list, gid: int = GID) -> int:
	sql, params = querify(memetoks, gid)
	return len(db.select(sql, params))


def wipe(gid: int) -> int:
	if not gid: raise Exception('wipe gid')
	for tbl in (DB['table_node'], DB['table_numb'], DB['table_name']):
		db.insert(f"DELETE FROM {tbl} WHERE gid=%s",[gid])
	return 1


###############################################################################
#                                  CLI
###############################################################################

# Execute and output an SQL query
def cli_sql(qry_sql):
	rows = db.select(qry_sql, [])
	for row in rows: print(row)


# Execute and output a Memelang query
def cli_query(memestr: str):
	memetoks = decode(memestr)
	print ("TOKENS:", memetoks)
	print ("QUERY:", encode(memetoks))

	memetoks = idecode(memestr)
	sql, params = querify(memetoks)
	full_sql = db.morfigy(sql, params)
	print(f"SQL: {full_sql}\n")

	# Execute query
	print(f"RESULTS:")
	print(query(memestr))
	print()
	print()

def cli_put(memestr: str):
	print(keyencode(put(memestr, GID), GID))
	print()
	print()

# Read a meme file and save it to DB
def cli_putfile(file_path):
	with open(file_path, 'r', encoding='utf-8') as f: print(keyencode(put(f.read(), GID), GID))
	

# Test various Memelang queries
def cli_qrytest():
	import copy

	queries=[
		'child',
		'CHILD =',
		'child parent',
		'child parent=',
		'child= parent=',
		'=JohnAdams',
		'parent=JOHNadams',
		'child[birthee',
		'child[birthee =',
		'child[birthee year>',
		'year==1732',
		'year=1732.0',
		'year>1700',
		'year<=1800',
		'year<=1800',
		'year>=1700',
		'child[birthee year>=1700',
	]
	errcnt=0

	for memestr in queries:
		memetoks=decode(memestr)
		print('Tokens:', memetoks)
		identify(memetoks)
		print('Idents:', memetoks)
		print('Query 1:', memestr)
		memetoks2 = copy.deepcopy(memetoks)
		memestr2 = memestr

		for i in range(2,4):
			memestr2 = keyencode(memetoks2).replace("\n", ";")
			memetoks2 = idecode(memestr2)
			print(f'Query {i}:', memestr2)


		sql, params = querify(memetoks)
		print('SQL: ', db.morfigy(sql, params))
		
		c1=count(memetoks)
		c2=count(memetoks2)
		print ('First Count:  ', c1)
		print ('Second Count: ', c2)

		if not c1 or c1!=c2 or c1>200:
			print()
			print('*** COUNT ERROR ABOVE ***')
			errcnt+=1

		print()
	print("ERRORS:", errcnt)
	print()


# Add database and user
def cli_dbadd():
	commands = [
		f"sudo -u postgres psql -c \"CREATE DATABASE {DB['name']};\"",
		f"sudo -u postgres psql -c \"CREATE USER {DB['user']} WITH PASSWORD '{DB['pass']}'; GRANT ALL PRIVILEGES ON DATABASE {DB['name']} to {DB['user']};\"",
		f"sudo -u postgres psql -c \"GRANT ALL PRIVILEGES ON DATABASE {DB['name']} to {DB['user']};\""
	]
	for command in commands:
		print(command)
		os.system(command)


# Add database table
def cli_tableadd():
	corp=I['cor']+1
	commands = [
		f"CREATE SEQUENCE {DB['table_seqn']} AS BIGINT START {corp} INCREMENT 1 CACHE 1;",
		f"SELECT setval('{DB['table_seqn']}', {corp}, false);",
		f"CREATE TABLE {DB['table_node']} (gid BIGINT, bid BIGINT, rid BIGINT, aid BIGINT, PRIMARY KEY (gid,bid,rid)); CREATE INDEX {DB['table_node']}_rid_idx ON {DB['table_node']} USING hash (rid); CREATE INDEX {DB['table_node']}_aid_idx ON {DB['table_node']} USING hash (aid);",
		f"CREATE TABLE {DB['table_numb']} (gid BIGINT, bid BIGINT, rid BIGINT, amt DOUBLE PRECISION, PRIMARY KEY (gid,bid,rid)); CREATE INDEX {DB['table_numb']}_rid_idx ON {DB['table_numb']} USING hash (rid); CREATE INDEX {DB['table_numb']}_amt_idx ON {DB['table_numb']} (amt);",
		f"CREATE TABLE {DB['table_name']} (gid BIGINT, bid BIGINT, rid BIGINT, alp VARCHAR(511), PRIMARY KEY (gid,bid,rid)); CREATE INDEX {DB['table_name']}_rid_idx ON {DB['table_name']} USING hash (rid); CREATE INDEX {DB['table_name']}_alp_idx ON {DB['table_name']} (LOWER(alp));",
		f"GRANT USAGE, UPDATE ON SEQUENCE {DB['table_seqn']} TO {DB['user']};",
		f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE {DB['table_node']} TO {DB['user']};",
		f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE {DB['table_numb']} TO {DB['user']};",
		f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE {DB['table_name']} TO {DB['user']};",
	]

	for command in commands: db.psql(command)


# Delete database table
def cli_tabledel():
	commands = [
		f"DROP SEQUENCE {DB['table_seqn']};",
		f"DROP TABLE IF EXISTS {DB['table_node']};",
		f"DROP TABLE IF EXISTS {DB['table_numb']};",
		f"DROP TABLE IF EXISTS {DB['table_name']};",
	]
	for command in commands: db.psql(command)

if __name__ == "__main__":
	LOCAL_DIR = os.path.dirname(os.path.abspath(__file__))

	cmd = sys.argv[1]
	if cmd == 'sql': cli_sql(sys.argv[2])
	elif cmd in ('query','qry','q','get','g'): cli_query(sys.argv[2])
	elif cmd == 'put': cli_put(sys.argv[2])
	elif cmd in ('file','import'): cli_putfile(sys.argv[2])
	elif cmd in ('dbadd','adddb'): cli_dbadd()
	elif cmd in ('tableadd','addtable'): cli_tableadd()
	elif cmd in ('tabledel','deltable'): cli_tabledel()
	elif cmd == 'qrytest': cli_qrytest()
	elif cmd == 'install':
		cli_dbadd()
		cli_tableadd()
	elif cmd == 'reinstall':
		cli_tabledel()
		cli_tableadd()
		if len(sys.argv)>2 and sys.argv[2]=='-presidents': cli_putfile(os.path.join(LOCAL_DIR,'presidents.meme'))
	elif cmd in ('fileall','allfile'):
		files = glob.glob(LOCAL_DIR+'/*.meme') + glob.glob(LOCAL_DIR+'/data/*.meme')
		for f in files: cli_putfile(f)
	else: sys.exit("Invalid command")
