#!/usr/bin/env python3

import sys
import os
import re
import glob
import psycopg2
from conf import DB


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
	'cor' : 2**30
}

# Lazy population for now
K = {value: key for key, value in I.items()}

RID, BID, AID, AMT, ALP = 'rid', 'bid', 'aid', 'amt', 'alp'
TBL = {RID: DB['table_node'], AID: DB['table_node'], AMT: DB['table_numb'], ALP: DB['table_name']}

FUNC, CLMN, TIER, OBEG, OEND = 0, 1, 2, 3, 4
TERM, TAND, TFWD, TREV, TIMP, TEND = 0, 1, 2, 3, 4, 5
VV, XO, XV, YO, YV = 0, 1, 2, 3, 4
VALS, OPRS = (XV, YV), (XO, YO)
MYV = 1
TLEN = 5

OPR = { # Each operator and its meaning
	None     : [None, None, None, None, None],
	I[' ']   : [XO, RID, TAND, False, False],
	I['>>']  : [XO, RID, TIMP, False, False],
	I[';']   : [XO, RID, TEND,  "\n", False],

	I['[']   : [XO, RID, TFWD, False, False],
	I[']']   : [XO, RID, TREV, False, False],

	I['=']   : [YO, AID, TERM, False, False],

	I['="'] : [YO, ALP, TERM, False, '"'],
	I['==']  : [YO, AMT, TERM, False, False],
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
# Output: statements as [[[XO, XV, YO, YV]], ...]
def decode(memestr: str) -> list:

	memestr = re.sub(r'\s*//.*$', '', memestr, flags=re.MULTILINE).strip() # Remove comments
	if len(memestr) == 0: raise Exception("Empty query provided.")

	statements, expressions = [], []
	terms = [None, None, None, None, None]
	operator = None

	parts = re.split(r'(?<!\\)"', ';'+memestr)
	for p, part in enumerate(parts):

		# Quote
		if p%2==1:
			terms[YO], terms[YV] = I['="'], part
			continue

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
					if completeness==INCOMPLETE: raise Exception(f"Invalid strtok {strtok}")

				if OPR[operator][TIER] >= TAND:
					if terms[VV]==MYV: expressions.append(terms)
					terms = [None, operator, None, None, None]
					if OPR[operator][TIER] >= TFWD:
						terms[VV]=MYV
						if OPR[operator][TIER] >= TIMP:
							if expressions: statements.append(expressions)
							expressions = []
				else:
					terms[VV]=MYV
					terms[OPR[operator][FUNC]]=operator

			# Key/Integer/Decimal
			else:
				if operator is None: raise Exception(f'Sequence error at {strtok}')
				if re.search(r'[^a-zA-Z0-9\.\-]', strtok): raise Exception(f"Unexpected '{strtok}' in {memestr}")

				# R=123.4 to R==123.4
				if ('.' in strtok or strtok.startswith('-')) and operator==I['=']:
					operator=I['==']
					terms[OPR[operator][FUNC]]=operator
					strtok=float(strtok)

				terms[OPR[operator][FUNC]+1]=strtok
				terms[VV]=MYV

			t+=1

	if terms[VV]==MYV: expressions.append(terms)
	if expressions: statements.append(expressions)

	normalize(statements)

	return statements

# Input: statements as [[[XO, XV, YO, YV]], ...]
# Output: Memelang string "operator1operand1operator2operand2"
def encode(statements: list) -> str:
	memestr = ''
	for s, expressions in enumerate(statements):
		for e, terms in enumerate(expressions):
			for t in OPRS:
				if terms[t] is None: continue
				memestr += K[terms[t]] if OPR[terms[t]][OBEG] is False else OPR[terms[t]][OBEG]
				if terms[t+1] is not None: memestr += str(terms[t+1])
				if OPR[terms[t]][OEND]: memestr += OPR[terms[t]][OEND]
	return memestr


# [[[XO, XV, YO, YV]], ...]
def normalize(statements: list[list]):
	for s, expressions in enumerate(statements):
		for e, terms in enumerate(expressions):
			if len(terms)!=TLEN: raise Exception(f"Term count error for at {s}:{e}")
			if terms[VV]!=MYV: raise Exception(f"Unkown term version at {s}:{e}")

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

			statements[s][e]=terms


###############################################################################
#                        DATABASE HELPER FUNCTIONS
###############################################################################

def select(sql: str, params: list = []) -> list:
	with psycopg2.connect(f"host={DB['host']} dbname={DB['name']} user={DB['user']} password={DB['pass']}") as conn:
		cursor = conn.cursor()
		cursor.execute(sql, params)
		rows=cursor.fetchall()
		return [list(row) for row in rows]


def insert(sql: str, params: list = []):
	with psycopg2.connect(f"host={DB['host']} dbname={DB['name']} user={DB['user']} password={DB['pass']}") as conn:
		cursor = conn.cursor()
		cursor.execute(sql, params)


def aggnum(col: str = 'aid', agg: str = 'MAX', table: str = None) -> int:
	if not table: table=DB['table_node']
	result = select(f"SELECT {agg}({col}) FROM {table}")
	return int(0 if not result or not result[0] or not result[0][0] else result[0][0])


def selectin(cols: dict = {}, table: str = None) -> list:
	if not table: table=DB['table_node']

	conds, params = [], []

	for col in cols:
		conds.append(f"{col} IN ("+ ','.join(['%s'] * len(cols[col])) +")")
		params.extend(cols[col])

	if not conds: return []

	return select(f"SELECT DISTINCT * FROM {table} WHERE " + ' AND '.join(conds), params)


# Conbine SQL and parameters into a string
def morfigy(sql: str, params: list) -> str:
    for param in params:
        rep = param.replace("'", "''") if isinstance(param, str) else str(param)
        sql = sql.replace("%s", rep, 1)
    return sql


# Input: string "John Adams"
# Output: lowercase underscored string "JohnAdams"
def slugify(string: str) -> str:
	return re.sub(r'[^a-zA-Z0-9]', '', string)


###############################################################################
#                           KEY <-> ID CONVERSIONS
###############################################################################

# Input list of key strings ['GeorgeWashington', 'JohnAdams']
# Load key->aids in I and K caches
# I['JohnAdams']=123
# K[123]='JohnAdams'
def identify(statements: list[list], gids: list[int] = []):
	if not gids: gids = [GID]
	allaids=I
	for gid in gids:
		if not KEYS.get(gid): KEYS[gid]={}
		for q, a in KEYS[gid].items(): allaids.setdefault(q.lower(), a)

	lookups={}

	for s, expressions in enumerate(statements):
		for e, terms in enumerate(expressions):
			for t in VALS:
				if isinstance(terms[t], str) and not allaids.get(terms[t]): lookups[terms[t].lower()]=1

	if lookups:
		rows=selectin({f'LOWER({ALP})':lookups.keys(), 'rid':[I['key']], 'gid':gids}, DB['table_name'])
		for row in rows: KEYS[int(row[0])][row[3]] = int(row[1])

		# must keep gid order
		for gid in gids:
			for q, a in KEYS[gid].items(): allaids.setdefault(q.lower(), a)

	for s, expressions in enumerate(statements):
		for e, terms in enumerate(expressions):
			for t in VALS:
				if isinstance(terms[t], str):
					iid = allaids.get(terms[t].lower(),0)
					if iid == 0: raise Exception(f"identify error at {terms[t]} in {terms} in {expressions}")
					statements[s][e][t]=iid


def keyify(statements: list[list], gids: list[int] = []) -> list:
	if not gids: gids = [GID]
	allstrs=K
	for gid in gids:
		if not KEYS.get(gid): KEYS[gid]={}
		for q, a in KEYS[gid].items(): allstrs.setdefault(a, q)

	lookups={}

	for s, expressions in enumerate(statements):
		for e, terms in enumerate(expressions):
			for t in VALS:
				if isinstance(terms[t], int) and not allstrs.get(terms[t]): lookups[terms[t]]=1

	if lookups:
		rows=selectin({'bid':lookups.keys(), 'rid':[I['key']], 'gid':gids}, DB['table_name'])
		for row in rows: KEYS[int(row[0])][row[3]] = int(row[1])

		# must keep gid order
		for gid in gids:
			for q, a in KEYS[gid].items(): allstrs.setdefault(a, q)

	for s, expressions in enumerate(statements):
		for e, terms in enumerate(expressions):
			for t in VALS:
				if isinstance(terms[t], int): statements[s][e][t]=allstrs[terms[t]]


# Run decode() and identify()
def idecode(memestr: str, gids: list[int] = []) -> list:
	statements = decode(memestr)
	identify(statements, gids)
	return statements


# Run keyify() and encode()
def keyencode(statements: list[list], gids: list[int] = []) -> str:
	keyify(statements, gids)
	return encode(statements)


###############################################################################
#                         MEMELANG -> SQL QUERIES
###############################################################################

# Input: tokens
# Output: "SELECT x FROM y WHERE z", [param1, param2, ...]
def selectify(expressions: list[list], gids: list[int] = []) -> tuple[str, list]:
	if not gids: gids = [GID]

	n       = 0
	bb      = [n]
	aa      = [n]
	gid     = str(int(gids[0]))
	tbl     = DB['table_node']
	where 	= f"n{n}.gid={gid}"
	groupby = f"n{n}.bid"
	join    = ''
	select  = ''
	params 	= []

	for terms in expressions:
		tier = OPR[terms[XO]][TIER]
		tbl = DB['table_node']
		acol = AID

		if terms[YO]:
			acol = OPR[terms[YO]][CLMN]
			tbl = TBL[acol]

		if tier == TEND:
			join 	+= f' FROM {tbl} n{n}'

		elif tier == TAND:
			n+=1
			bb.append(n)
			join	+= f" JOIN {tbl} n{bb[-1]} ON n{aa[-1]}.bid=n{bb[-1]}.bid"
			if acol == AID:
				where   += f" AND (n{aa[-1]}.aid!=n{bb[-1]}.aid OR n{aa[-1]}.rid!=n{bb[-1]}.rid)"

		elif tier == TFWD:
			n+=1
			aa.append(n)
			bb.append(n)
			join	+= f" JOIN {tbl} n{bb[-1]} ON n{bb[-2]}.aid=n{bb[-1]}.aid"
			where	+= f" AND n{bb[-1]}.gid={gid} AND n{bb[-2]}.bid!=n{bb[-1]}.bid"
			groupby += f", n{bb[-1]}.rid, n{bb[-1]}.bid"

		elif tier == TREV:
			select	+= ", ']'"
			aa.pop()
			continue

		if terms[XV] is not None:
			where += f" AND n{n}.rid=%s"
			params.append(terms[XV])

		if acol == AID:
			select 	+= f", string_agg(DISTINCT '{K[terms[XO]]}' || n{n}.rid || '=' || n{n}.aid, '')"
			if terms[YV] is not None:
				where += f" AND n{n}.aid=%s"
				params.append(terms[YV])
				groupby += f", n{n}.rid, n{n}.aid"

		elif acol == ALP:
			select 	+= f", ' ', string_agg(DISTINCT n{n}.rid || '=\"' || n{n}.alp || '\"', '')"
			if terms[YV] is not None:
				where += f" AND LOWER(n{n}.alp) LIKE %s"
				params.append(terms[YV].lower())

		elif acol == AMT: 
			select 	+= f", ' ', string_agg(DISTINCT n{n}.rid || '==' || n{n}.amt, '')"
			if terms[YV] is not None:
				cpr = '=' if terms[YO] == I['=='] else K[terms[YO]]
				where += f" AND n{n}.amt{cpr}%s"
				params.append(terms[YV])


	return f"SELECT CONCAT({select[1:]}) AS raq {join} WHERE {where} GROUP BY {groupby}", params


# Input: Memelang query string
# Output: SQL query string
def querify(statements: list[list], gids: list[int] = []) -> tuple[str, list]:
	selects, params = [], []
	for s, expressions in enumerate(statements):
		qry_select, qry_params = selectify(expressions, gids)
		selects.append(qry_select)
		params.extend(qry_params)
	return ' UNION '.join(selects), params


# Input: Memelang string
# Saves to DB
# Output: Memelang string
def put (memestr: str, gids: list[int] = []) -> str:
	
	if not gids: gids = [GID]
	gid = gids[-1]
	if gid not in KEYS: KEYS[gid]={}

	statements = decode(memestr)
	maxid = aggnum('bid', 'MAX', DB['table_node']) or I['cor']

	sqls = {DB['table_node']:[], DB['table_name']:[], DB['table_numb']:[]}
	params = {DB['table_node']:[], DB['table_name']:[], DB['table_numb']:[]}

	# NEW KEY NAMES

	newkeys = {}
	l2u = {}

	for s, expressions in enumerate(statements):
		for e, terms in enumerate(expressions):
			for t in VALS:
				if isinstance(terms[t], str):
					if iid := KEYS[gid].get(terms[t]): statements[s][e][t]=iid
					elif re.search(r'[^a-zA-Z0-9]', terms[t]): raise Exception(f'Invalid key {terms[t]}')
					else: 
						alp = slugify(terms[t])
						alpl = alp.lower()
						if not newkeys.get(alpl):
							newkeys[alpl] = 0
							l2u[alpl] = alp

	if newkeys:
		# Unique check keys
		rows=selectin({'gid':[gid], 'rid':[I['key']], f'LOWER({ALP})':newkeys.keys()}, DB['table_name'])
		for row in rows:
			alp=row[3]
			alpl = alp.lower()
			if newkeys.get(alpl):
				if int(row[1]) == int(newkeys[alpl]) or newkeys[alpl] is None: newkeys.pop(alpl, 0)
				else: raise Exception(f"Duplicate key {alp} for new {newkeys[alpl]} and old {row[1]}")

		# Write new keys
		for alpl in newkeys:
			alp = l2u[alpl]

			if re.search(r'[^a-zA-Z0-9]', alpl) or not re.search(r'[a-zA-Z]', alpl):
				raise Exception(f'Invalid key {alpl}')

			aid = newkeys[alpl]
			if not aid:
				maxid += 1
				aid = maxid
			elif aid<=I['cor']: raise Exception(f'Invalid id number {aid}')

			KEYS[gid][alp]=aid
			sqls[DB['table_name']].append("(%s,%s,%s,%s)")
			params[DB['table_name']].extend([gid, aid, I['key'], alp])

		# Swap missing keys for new IDs
		identify(statements, gids)
	
	# NEW MEMES
	for s, expressions in enumerate(statements):
		maxid+=1
		for e, terms in enumerate(expressions):
			col = OPR[terms[YO]][CLMN]
			if col == AID: tbl = DB['table_node']
			elif col == AMT: tbl = DB['table_numb']
			elif col == ALP: tbl = DB['table_name']
			else: raise Exception
			params[tbl].extend([gid, maxid, terms[XV], terms[YV]])
			sqls[tbl].append('(%s,%s,%s,%s)')

	for tbl in params:
		if params[tbl]: insert(f"INSERT INTO {tbl} VALUES " + ','.join(sqls[tbl]) + " ON CONFLICT DO NOTHING", params[tbl])

	return keyencode(statements, [gid])


# Input: Memelang query string
# Output: Memelang results string
def query(memestr: str = None, gids: list[int] = []) -> str:
	if not gids: gids = [GID]

	sql, params = querify(idecode(memestr, gids), gids)
	res = select(sql, params)

	if not res: return ''

	responsestr=''
	for row in res: responsestr+=row[0]

	return keyencode(decode(responsestr), gids)


# Input: Memelang query string
# Output: Integer count of resulting memes
def count(memestr: str, gids: list[int] = []) -> int:
	if not gids: gids = [GID]
	sql, params = querify(idecode(memestr, gids), gids)
	return len(select(sql, params))


###############################################################################
#                                  CLI
###############################################################################

# Execute and output an SQL query
def cli_sql(qry_sql):
	rows = select(qry_sql, [])
	for row in rows: print(row)


# Execute and output a Memelang query
def cli_query(memestr):
	tokens = decode(memestr)
	print ("TOKENS:", tokens)
	print ("QUERY:", encode(tokens))

	tokens = idecode(memestr)
	sql, params = querify(tokens)
	full_sql = morfigy(sql, params)
	print(f"SQL: {full_sql}\n")

	# Execute query
	print(f"RESULTS:")
	print(query(memestr))
	print()
	print()


# Read a meme file and save it to DB
def cli_putfile(file_path):
	with open(file_path, 'r', encoding='utf-8') as f: print(put(f.read()))
	

# Test various Memelang queries
def cli_qrytest():
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
		'child[birthee year==',
		'year==1732',
		'year=1732.0',
		'year>1700',
		'year<=1800',
		'year<=1800',
		'year>=1700',
		'child[birthee year>=1700',
		'office officer[child parent[birthee year<=1800]] jurisdiction=USA',
	]
	errcnt=0

	for memestr in queries:
		print('Tokens:', decode(memestr))
		print('Query 1:', memestr)
		memestr2=memestr

		for i in range(2,4):
			memestr2 = keyencode(idecode(memestr2)).replace("\n", ";")
			print(f'Query {i}:', memestr2)

		tokens = idecode(memestr)
		sql, params = querify(tokens)
		print('SQL: ', morfigy(sql, params))
		
		c1=count(memestr)
		c2=count(memestr2)
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
	commands = [
		f"sudo -u postgres psql -d {DB['name']} -c \"CREATE TABLE {DB['table_node']} (gid BIGINT, bid BIGINT, rid BIGINT, aid BIGINT, PRIMARY KEY (gid,bid,rid)); CREATE INDEX {DB['table_node']}_rid_idx ON {DB['table_node']} USING hash (rid); CREATE INDEX {DB['table_node']}_aid_idx ON {DB['table_node']} USING hash (aid);\"",
		f"sudo -u postgres psql -d {DB['name']} -c \"CREATE TABLE {DB['table_numb']} (gid BIGINT, bid BIGINT, rid BIGINT, amt DOUBLE PRECISION, PRIMARY KEY (gid,bid,rid)); CREATE INDEX {DB['table_numb']}_rid_idx ON {DB['table_numb']} USING hash (rid); CREATE INDEX {DB['table_numb']}_amt_idx ON {DB['table_numb']} (amt);\"",
		f"sudo -u postgres psql -d {DB['name']} -c \"CREATE TABLE {DB['table_name']} (gid BIGINT, bid BIGINT, rid BIGINT, alp VARCHAR(511), PRIMARY KEY (gid,bid,rid)); CREATE INDEX {DB['table_name']}_rid_idx ON {DB['table_name']} USING hash (rid); CREATE UNIQUE INDEX {DB['table_name']}_amt_idx ON {DB['table_name']} (LOWER(alp));\"",
		f"sudo -u postgres psql -d {DB['name']} -c \"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE {DB['table_node']} TO {DB['user']};\"",
		f"sudo -u postgres psql -d {DB['name']} -c \"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE {DB['table_numb']} TO {DB['user']};\"",
		f"sudo -u postgres psql -d {DB['name']} -c \"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE {DB['table_name']} TO {DB['user']};\"",
	]

	for command in commands:
		print(command)
		os.system(command)


# Delete database table
def cli_tabledel():
	commands = [
		f"sudo -u postgres psql -d {DB['name']} -c \"DROP TABLE {DB['table_node']};\"",
		f"sudo -u postgres psql -d {DB['name']} -c \"DROP TABLE {DB['table_numb']};\"",
		f"sudo -u postgres psql -d {DB['name']} -c \"DROP TABLE {DB['table_name']};\"",
	]
	for command in commands:
		print(command)
		os.system(command)

if __name__ == "__main__":
	LOCAL_DIR = os.path.dirname(os.path.abspath(__file__))

	cmd = sys.argv[1]
	if cmd == 'sql': cli_sql(sys.argv[2])
	elif cmd in ('query','qry','q','get','g'): cli_query(sys.argv[2])
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
		if len(sys.argv)>2 and sys.argv[2]=='-presidents': cli_putfile(os.path.join(LOCAL_DIR,'presidents.mq'))
	elif cmd in ('fileall','allfile'):
		files = glob.glob(LOCAL_DIR+'/*.mq') + glob.glob(LOCAL_DIR+'/data/*.mq')
		for f in files: cli_putfile(f)
	else: sys.exit("Invalid command")
