#!/usr/bin/env python3

# memelang.net

import sys, os, re, glob, db, random
from db import DB

###############################################################################
#						   CONSTANTS & GLOBALS
###############################################################################

DEFTBL = 'meme'
DEFGRP = 'main'

MEME_TABLE = f'''
CREATE TABLE IF NOT EXISTS {DEFTBL} (grp TEXT, rel TEXT, alp TEXT, amt DOUBLE PRECISION, bid BIGINT);
CREATE INDEX IF NOT EXISTS {DEFTBL}_bid_idx ON {DEFTBL} (bid);
CREATE INDEX IF NOT EXISTS {DEFTBL}_amt_idx ON {DEFTBL} (amt) WHERE amt IS NOT NULL;
CREATE INDEX IF NOT EXISTS {DEFTBL}_grp_idx ON {DEFTBL} USING HASH ((LOWER(grp)));
CREATE INDEX IF NOT EXISTS {DEFTBL}_rel_idx ON {DEFTBL} USING HASH ((LOWER(rel)));
CREATE INDEX IF NOT EXISTS {DEFTBL}_alp_idx ON {DEFTBL} USING HASH ((LOWER(alp))) WHERE alp IS NOT NULL;
'''

SQLPARAM = [None, None, None, None, None]
SQLPLACE = '(%s,%s,%s,%s,%s)'
GRP, REL, ALP, AMT, BID = 'grp', 'rel', 'alp', 'amt', 'bid'
COLORD = {GRP: 0, REL: 1, ALP: 2, AMT: 3, BID: 4}

# mov = meme operator-value pair = [OPR, VAL]
# movs = mov list = [[OPR, VAL], ...]
# moks = meme tokens = mov list list = [[[OPR, VAL], ...], ...]
OPR, VAL = 0, 1
SCOL, SLNK, SEQL, SOUT = 0, 1, 2, 3
LVAL, LAND, LFWD, LREV, LIMP, LEND = 0, 1, 2, 3, 4, 5
SPC=' ' # and relation equals
END=';'

OPERS = {
	END  : (REL, LEND, None, "\n"),
	SPC  : (REL, LAND, '=', ' '),
	' !' : (REL, LAND, '!=', ' !'),
	'['  : (REL, LFWD, '=', '['),
	']'  : (REL, LREV, None, ']'),
	'='  : (AMT, LVAL, '=', '='),
	'!=' : (AMT, LVAL, '!=', '!='),
	'>'  : (AMT, LVAL, '>', '>'),
	'<'  : (AMT, LVAL, '<', '<'),
	'>=' : (AMT, LVAL, '>=', '>='),
	'<=' : (AMT, LVAL, '<=', '<='),
	'{'  : (GRP, LFWD, '=', '{'),
	'}'  : (GRP, LREV, None, '}'),
	':'  : (BID, LVAL, '=', ':'),
	'!:' : (BID, LVAL, '!=', '!:'),
}

MEME_OPR_RE = re.compile(r"""(!=|>=|<=|\s!|\s|[\]\[\}\{:;!><=])([^\]\[\}\{:;!><=\s"]*)""") # Split into operator, value
MEME_END_RE = re.compile(r'\s*[;\n]+\s*') # Remove multiple semicolons
MEME_SPC_RE = re.compile(r'[\\&\s]+') # Backslash, whitespace, and ampersand are SPC
MEME_QOT_RE = re.compile(r'(?<!\\)"') # String between quotes
MEME_STR_RE = re.compile(r'[^0-9\.\-\+]') # Matches non numeric chars, must be string
MEME_ALP_RE = re.compile(r'[^a-zA-Z0-9]') # Complex string must be wrapped in quotes


###############################################################################
#					   MEMELANG STRINGING PROCESSING
###############################################################################

# Input: Memelang string as "operator1value1operator2value2"
# Output: moks as [[[OPR, VAL]], ...]
def decode(memestr: str) -> list:

	memestr = re.sub(r'\s*//.*$', '', memestr, flags=re.MULTILINE).strip() # Remove comments
	memestr = re.sub(r'""', '', memestr) # Remove empty quotes

	if len(memestr) == 0: raise Exception('Empty memelang string')

	moks, movs = [], []

	# Split by quotes, skip inside the quotes, parse outside of the quotes
	parts = MEME_QOT_RE.split(' '+memestr)
	for p, part in enumerate(parts):

		# Assign string inside quotes straight to ="value"
		if p%2==1:
			movs.append(['=', part.replace(r'\"', '"')])
			continue

		# Parse string outside of quotes
		part = MEME_END_RE.sub(END, part)	
		part = MEME_SPC_RE.sub(SPC, part)

		for opr, val in MEME_OPR_RE.findall(part):
			opr = opr or SPC
			
			if opr == SPC and val == '': continue
			elif not OPERS.get(opr): raise Exception(f"invalid operator: {opr}")

			# Value
			if val == '': val = None
			elif MEME_STR_RE.search(val): pass # already a string
			elif '.' in val: val=float(val)
			else: val=int(val)

			# Operator ends statement
			if OPERS[opr][SLNK] == LEND:
				moks.append(movs)
				movs = []
				if val is not None: movs.append([SPC, val])

			# Operator has no value
			elif OPERS[opr][SLNK] in (LIMP, LREV):
				movs.append([opr, None])
				if val is not None: movs.append([SPC, val])

			# Operator-value pair
			else: movs.append([opr, val])

	if movs: moks.append(movs)

	return moks

# Input: moks as [[[OPR, VAL]], ...]
# Output: Memelang string "operator1value1operator2value2"
def encode(moks: list) -> str:
	memestr = ''
	for movs in moks:
		for opr, val in movs:
			memestr += str(OPERS[opr][SOUT])
			if val is None: continue
			elif isinstance(val, str) and MEME_ALP_RE.search(val): memestr += '"' + val.replace('"', r'\"') + '"'
			else: memestr += str(val)
		memestr += OPERS[END][SOUT]

	return memestr


###############################################################################
#						 MEMELANG -> SQL QUERIES
###############################################################################

def selectify(movs: list[list], grp: str = None, tname: str = None) -> tuple[str, list]:

	if not grp: grp = DEFGRP
	elif MEME_ALP_RE.search(grp): raise ValueError('selectify grp')

	tname		= tname or DEFTBL
	n		 	= 0
	bb		 	= [n]
	aa		 	= [n]
	where 	 	= f"n{n}.grp='{grp}'"
	groupby  	= f"n{n}.grp,n{n}.bid"
	joins	 	= f''
	select		= f"""';{{' || n{n}.grp || ':' || n{n}.bid"""
	params		= []

	for i, (opr, val) in enumerate(movs):
		link, col, eql = OPERS[opr][SLNK], OPERS[opr][SCOL], OPERS[opr][SEQL]

		if i==0:
			joins += f' FROM {tname} n{n}'

		elif link == LAND:
			n+=1
			bb.append(n)
			joins += f" LEFT JOIN {tname} n{bb[-1]} ON n{n}.grp='{grp}' AND n{aa[-1]}.bid=n{bb[-1]}.bid"
			joins += f" AND (n{aa[-1]}.{ALP}!=n{bb[-1]}.{ALP} OR n{aa[-1]}.{AMT}!=n{bb[-1]}.{AMT} OR n{aa[-1]}.rel!=n{bb[-1]}.rel)"

		elif link == LFWD:
			n+=1
			aa.append(n)
			bb.append(n)
			select	 += f""", ' {{' || n{n}.grp || ':' || n{n}.bid"""
			joins	 += f" JOIN {tname} n{n} ON n{bb[-2]}.{ALP}=n{n}.{ALP} AND n{n}.grp='{grp}' AND n{bb[-2]}.bid!=n{n}.bid"
			groupby  += f", n{n}.grp, n{n}.bid"

		elif link == LREV:
			select	+= ", '}'"
			aa.pop()
			continue

		if link != LVAL:
			select += f''', string_agg(DISTINCT ' ' || n{n}.rel || '=' || CONCAT(n{n}.{AMT}, '"',  n{n}.{ALP}, '"'), '')'''

		if val is None: continue
		elif isinstance(val, str):
			col = ALP if col == AMT else col
			where += f" AND LOWER(n{n}.{col}){eql}%s"
			params.append(val.lower())
		else:
			where += f" AND n{n}.{col}{eql}%s"
			params.append(val)

	return f"SELECT CONCAT({select}) AS mstr {joins} WHERE {where} GROUP BY {groupby}", params


def sqlify(moks: list[list], grp: str = None, tname: str = None) -> tuple[str, list]:
	selects, params = [], []
	for s, movs in enumerate(moks):
		qry_select, qry_params = selectify(movs, grp, tname)
		selects.append(qry_select)
		params.extend(qry_params)
	return ' UNION '.join(selects), params


def put (moks: list[list], grp: str, tname: str = DEFTBL) -> list[list]:
	
	if not grp: raise Exception('put grp')

	rows, params = [], []

	for movs in moks:

		values={BID:[], GRP:[], REL:[], AMT:[]}

		for opr, val in movs:
			for v in values: values[v].append(None)
			values[OPERS[opr][SCOL]][-1]=val

		values[BID] = [x for x in values[BID] if x is not None]
		values[GRP] = [x for x in values[GRP] if x is not None]

		if len(values[BID])>1: raise Exception(f'redundant bid')
		if len(values[GRP])>1: raise Exception(f'redundant grp')

		if not values[BID]: values[BID].append(2**20 + random.getrandbits(62))
		if not values[GRP]: values[GRP].append(grp)

		gbrow = SQLPARAM.copy()
		gbrow[COLORD[BID]], gbrow[COLORD[GRP]] = values[BID][0], values[GRP][0]

		for i,rel in enumerate(values[REL]):
			if rel is None: continue
			row = gbrow.copy()
			row[COLORD[REL]] = rel
			row[COLORD[ALP if isinstance(values[AMT][i+1], str) else AMT]] = values[AMT][i+1]
			rows.append(SQLPLACE)
			params.extend(row)

	if rows:
		sqls=f"INSERT INTO {DEFTBL} VALUES " + ','.join(rows) + " ON CONFLICT DO NOTHING"
		db.insert(sqls, params)

	return moks


def get(moks: list[list], grp: str = None, tname: str = None) -> list[list]:
	res = db.select(*sqlify(moks, grp))
	if not res: return []

	responsestr=''
	for row in res: responsestr+=row[0]
	#print(responsestr)
	return decode(responsestr)


# Input: Memelang query string
# Output: Integer count of resulting memes
def count(moks: list[list], grp: str = DEFGRP) -> int:
	return len(db.select(*sqlify(moks, grp)))


def jobify(memestr: str, morekeys: list = []) -> dict:
	job = {'*':None, 'j': None, 'g': None}

	for k in morekeys: job[k] = None
	lines = re.split(r'[\n;]+', memestr)
	if not lines or not lines[0]: return job

	pairs = re.split(r'\s+', lines[0])
	for kv in pairs:
		if '=' not in kv: continue
		key, val = kv.split('=', 1)
		if not val or not key or key not in job: continue
		elif job[key] is not None: raise Exception(f'redundant value for {key}=')
		elif key == 'j': val = val.lower()
		job[key] = val
		job['*'] = True

	return job


# Input: Memelang query string
# Output: Memelang results string
def query(memestr: str = None) -> str:
	moks = decode(memestr)

	if not moks: return ''

	job=jobify(memestr)
	if job['*']: moks.pop(0)
	if not job['j']: job['j'] = 'get'
	if not job['g']: job['g'] = DEFGRP

	if job['j'] == 'put': return encode(put(moks, job['g']))
	else:
		if job['j'] == 'get': return encode(get(moks, job['g']))
		elif job['j'] == 'cnt': return 'amt=' + str(count(moks, job['g']))
		elif job['j'].startswith('del'): return 'amt=' + str(deljob(job['j'], moks, job['g']))
		else: raise Exception('invalid job')


###############################################################################
#								  CLI
###############################################################################

# Execute and output an SQL query
def cli_sql(qry_sql):
	rows = db.select(qry_sql, [])
	for row in rows: print(row)

# Execute and output a Memelang query
def cli_q(memestr: str):
	print(query(memestr))
	print()
	print()

# Execute and output a Memelang query
def cli_query(memestr: str):
	moks = decode(memestr)
	print ("TOKENS:", moks)
	print ("QUERY:", encode(moks).replace('\n', ';'))

	sql, params = sqlify(moks)
	full_sql = db.morfigy(sql, params)
	print(f"SQL: {full_sql}\n")

	# Execute query
	print(f"RESULTS:")
	print(query(memestr))
	print()
	print()

def cli_put(memestr: str):
	print(encode(put(decode(memestr), DEFGRP)))
	print()
	print()

# Read a meme file and save it to DB
def cli_putfile(file_path):
	with open(file_path, 'r', encoding='utf-8') as f: print(encode(put(decode(f.read()), DEFGRP)))
	

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
		'child[eventee',
		'child[eventee =',
		'child[eventee year>',
		'year==1732',
		'year=1732.0',
		'year>1700',
		'year<=1800',
		'year<=1800',
		'year>=1700',
		'child[eventee year>=1700',
	]
	errcnt=0

	for memestr in queries:
		moks=decode(memestr)
		print('Tokens:', moks)
		print('Query 1:', memestr)
		moks2 = copy.deepcopy(moks)
		memestr2 = memestr

		for i in range(2,4):
			memestr2 = encode(moks2).replace("\n", ";")
			moks2 = decode(memestr2)
			print(f'Query {i}:', memestr2)


		sql, params = sqlify(moks)
		print('SQL: ', db.morfigy(sql, params))
		
		c1=count(moks)
		c2=count(moks2)
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
		f"CREATE DATABASE {DB['name']};",
		f"CREATE USER {DB['user']} WITH PASSWORD '{DB['pass']}';",
		f"GRANT ALL PRIVILEGES ON DATABASE {DB['name']} to {DB['user']};"
	]
	for command in commands: db.psql(command)


# Add database table
def cli_tableadd():
	commands = MEME_TABLE.split(';') + [f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE {DEFTBL} TO {DB['user']};"]
	for command in commands:
		if command: db.psql(command)


# Delete database table
def cli_tabledel():
	commands = [f"DROP TABLE IF EXISTS {DEFTBL};"]
	for command in commands: db.psql(command)

if __name__ == "__main__":
	LOCAL_DIR = os.path.dirname(os.path.abspath(__file__))

	cmd = sys.argv[1]
	if cmd == 'sql': cli_sql(sys.argv[2])
	elif cmd == 'q': cli_q(sys.argv[2])
	elif cmd in ('query','qry'): cli_query(sys.argv[2])
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
