#!/usr/bin/env python3

import sys, os, re, glob, db
from db import DB


###############################################################################
#						   CONSTANTS & GLOBALS
###############################################################################

DB['table_seqn']='seqn'
DB['table_meme']='meme'

# Default graph ID
DEFGID = 999

# Global dictionary to cache key->id mappings
KEYS = {}
I = {
	'nam' : 2**29 + 0,
	'key' : 2**29 + 1,
	'tit' : 2**29 + 2,
	'cor' : 2**30
}

# miad = meme triad = [OPER, VAl1, VAL2]
# mexps = meme expressions = [[OPER, VAl1, VAL2], ...]
# mokens = meme tokens = [[[OPER, VAl1, VAL2], ...], ...]
OPER, VAL1, VAL2 = 0, 1, 2

# For decode()
SCOMP, SFNC, SCOL, SJON, SBEG, SEND = 0, 1, 2, 3, 4, 5
INCOMPLETE, SEMICOMPLETE, COMPLETE = 0, 1, 2
OPR1, OPR2 = VAL1+2, VAL2+2 # OPR1 and OPR2 combine into OPER
PLACEHOLDER = '(%s,%s,%s,%s,%s,%s)'
GID, BID, RID, AID, AMT, ALP = 'gid', 'bid', 'rid', 'aid', 'amt', 'alp'
oGID, oBID, oRID, oAID, oAMT, oALP = 0, 1, 2, 3, 4, 5
LNON, LAND, LFWD, LREV, LIMP, LBEG = 0, 1, 2, 3, 4, 5

OPRSTR = {
	'!'   : (INCOMPLETE, None, None, None, None, None),
	'>'   : (SEMICOMPLETE, OPR2, AMT, LNON, '>', None),
	'<'   : (SEMICOMPLETE, OPR2, AMT, LNON, '<', None),
	'='   : (SEMICOMPLETE, OPR2, AID, LNON, '=', None),
	';'   : (SEMICOMPLETE, OPR1, RID, LBEG, "\n", None),
	'=='  : (COMPLETE, OPR2, AMT, LNON, '=', None),
	'!='  : (COMPLETE, OPR2, AMT, LNON, '!=', None),
	'>='  : (COMPLETE, OPR2, AMT, LNON, '>=', None),
	'<='  : (COMPLETE, OPR2, AMT, LNON, '<=', None),
	' '   : (COMPLETE, OPR1, RID, LAND, ' ', None),
	'['   : (COMPLETE, OPR1, RID, LFWD, '[', None),
	']'   : (COMPLETE, OPR1, RID, LREV, ']', None),
	';{'  : (COMPLETE, OPR1, GID, LBEG, '\n{', None),
	'{'   : (COMPLETE, OPR1, GID, LFWD, '{', None),
	'}'   : (COMPLETE, OPR1, GID, LREV, '}', None),
	':'   : (COMPLETE, OPR2, BID, LNON, ':', None),

	'="'  : (COMPLETE, OPR2, ALP, LNON, '="', '"'),
}

LINK, COL1, COL2, EQL1, EQL2, OBEG, OMID, OEND = 0, 1, 2, 3, 4, 5, 6, 7
CV = ((COL1, VAL1), (COL2, VAL2))

# Lazy population for now
OPERS={}
ocnt=0
for k,v in OPRSTR.items():
	newop=[None, None, None, None, None, None, None, None, None]

	if v[SFNC]==OPR2:
		ocnt+=1
		newop[COL2], newop[LINK], newop[EQL2], newop[OMID], newop[OEND] = v[SCOL], LNON, v[SBEG], v[SBEG], v[SEND]
		OPERS[ocnt], I[k], = newop[:], ocnt

	elif v[SFNC]==OPR1:
		ocnt+=1
		newop[COL1], newop[LINK], newop[OBEG], newop[EQL1] = v[SCOL], v[SJON], v[SBEG], '='
		OPERS[ocnt], I[k] = newop[:], ocnt

		if newop[LINK]==LREV: continue

		for k2,v2 in OPRSTR.items():
			if v2[SFNC]!=OPR2: continue
			ocnt+=1
			newop[COL2], newop[EQL2], newop[OMID], newop[OEND] = v2[SCOL], v2[SBEG], v2[SBEG], v2[SEND]
			OPERS[ocnt], I[f"{k} {k2}"] = newop[:], ocnt


#for k in OPERS: print(f"{k} {OPERS[k]}")

# Lazy population for now
K = {value: key for key, value in I.items()}

###############################################################################
#					   MEMELANG STRINGING PROCESSING
###############################################################################

# Input: Memelang string as "operator1operand1operator2operand2"
# Output: mokens as [[[OPER, VAl1, VAL2]], ...]
def decode(memestr: str) -> list:

	memestr = re.sub(r'\s*//.*$', '', memestr, flags=re.MULTILINE).strip() # Remove comments
	if len(memestr) == 0: raise Exception('api=err fld=qry msg=empty')

	mokens, mexps, mquad = [], [], [None, None, None, None, None]
	func = None

	parts = re.split(r'(?<!\\)"', ';'+memestr)
	for p, part in enumerate(parts):

		# Quote
		if p%2==1:
			mquad[OPR2], mquad[VAL2] = '="', part
			continue

		part = re.sub(r'\s*\\\s*', ' ', part)			# Backslash is same line
		part = re.sub(r'[;\n]+', ';', part)				# Newlines are semicolons
		part = re.sub(r'\s+', ' ', part)				# Remove multiple spaces
		part = re.sub(r'\s+(?=[\]\[&])', '', part)		# Remove spaces before operators
		part = re.sub(r'\s*;+\s*', ';', part)			# Remove multiple semicolons
		part = re.sub(r';+$', '', part)					# Remove ending semicolon

		# Split by operator characters
		strtoks = re.split(r'([\]\[\}\{:;!><=\s])', part)
		tlen = len(strtoks)
		t = 0
		while t<tlen:
			strtok=strtoks[t]

			# Skip empty
			if len(strtok)==0: pass

			# Operator
			elif strtok in OPRSTR:

				# We might want to rejoin two sequential operator characters, such as > and =
				completeness, func = OPRSTR[strtok][0:2]
				if completeness!=COMPLETE:
					for n in (1,2):
						if t<tlen-n and len(strtoks[t+n]):
							if strtok+strtoks[t+n] in OPRSTR:
								strtok += strtoks[t+n]
								completeness, func = OPRSTR[strtok][0:2]
								t+=n
							break
					if completeness==INCOMPLETE: raise Exception(f'api=err fld=strtok msg=invalid val="{strtok}"')

				if mquad[func]:

					okey = f"{mquad[OPR1]} {mquad[OPR2]}" if (mquad[OPR1] is not None and mquad[OPR2] is not None) else (mquad[OPR1] or mquad[OPR2])
					if okey not in I or I[okey] not in OPERS: raise Exception(f"decode okey error on '{okey}'")

					if OPERS[I[okey]][LINK] >= LIMP:
						if mexps: mokens.append(mexps)
						mexps = []

					mexps.append([I[okey], mquad[VAL1], mquad[VAL2]])
					mquad = [None, None, None, None, None]

				mquad[func]=strtok

			# Key/Integer/Decimal
			else:
				if func is None: raise Exception(f'Sequence error at {strtok}')
				elif re.search(r'[^a-zA-Z0-9\.\-]', strtok): raise Exception(f"Unexpected '{strtok}' in {memestr}")
				elif func==OPR2:
					# R=a1234
					if re.fullmatch(r'a[0-9]+', strtok): strtok=int(strtok[1:])

					# R=1234
					elif (re.search(r'[0-9]', strtok) and not re.search(r'[a-zA-Z]', strtok)):
						strtok=float(strtok)
						if mquad[OPR2]=='=': mquad[OPR2]='=='

				mquad[func-2]=strtok
			t+=1

	if mquad[VAL1] or mquad[OPR2]:
		okey = f"{mquad[OPR1]} {mquad[OPR2]}" if (mquad[OPR1] is not None and mquad[OPR2] is not None) else (mquad[OPR1] or mquad[OPR2])
		if okey not in I or I[okey] not in OPERS: raise Exception(f"decode okey error2 on '{okey}'")

		if OPERS[I[okey]][LINK] >= LIMP:
			if mexps: mokens.append(mexps)
			mexps = []

		mexps.append([I[okey], mquad[VAL1], mquad[VAL2]])

	if mexps: mokens.append(mexps)

	normalize(mokens)

	return mokens

# Input: mokens as [[[OPER, VAl1, VAL2]], ...]
# Output: Memelang string "operator1operand1operator2operand2"
def encode(mokens: list) -> str:
	memestr = ''
	for mexps in mokens:
		for miad in mexps:
			for v in (OPERS[miad[OPER]][OBEG], miad[VAL1], OPERS[miad[OPER]][OMID], miad[VAL2], OPERS[miad[OPER]][OEND]):
				if v: memestr += str(v)

	return memestr


# [[[OPER, VAl1, VAL2]], ...]
def normalize(mokens: list[list]):
	for s, mexps in enumerate(mokens):
		for e, miad in enumerate(mexps):

			# Clean all
			for t in range(3):
				if isinstance(miad[t], str):
					if miad[t].isdigit(): miad[t]=int(miad[t])
				elif isinstance(miad[t], bool): miad[t]=None

			# Numeric value
			if OPERS[miad[OPER]][COL2]==AMT and miad[VAL2] is not None:
				try: miad[VAL2] = float(miad[VAL2])
				except ValueError: raise Exception(f"String operator error for {miad[VAL2]} at {s}:{e}")

			mokens[s][e]=miad


###############################################################################
#						   KEY <-> ID CONVERSIONS
###############################################################################

# Input list of key strings ['GeorgeWashington', 'JohnAdams']
# Load key->aids in I and K caches
# I['JohnAdams']=123
# K[123]='JohnAdams'
def identify(mokens: list[list], gid: int = DEFGID):
	allaids=I
	if not KEYS.get(gid): KEYS[gid]={}
	for q, a in KEYS[gid].items(): allaids.setdefault(q.lower(), a)

	lookups={}

	for s, mexps in enumerate(mokens):
		for e, miad in enumerate(mexps):
			for col, val in CV:
				if OPERS[miad[OPER]][col] in (AID,RID) and isinstance(miad[val], str) and not allaids.get(miad[val]): lookups[miad[val].lower()]=1
	
	if lookups:
		rows=db.selectin({f'LOWER({ALP})':lookups.keys(), 'rid':[I['key']], 'gid':[gid]}, DB['table_meme'])
		for row in rows: 
			KEYS[int(row[oGID])][row[oALP]] = int(row[oBID])
			allaids.setdefault(row[oALP].lower(), int(row[oBID]))

	for s, mexps in enumerate(mokens):
		for e, miad in enumerate(mexps):
			for col, val in CV:
				if OPERS[miad[OPER]][col] in (AID,RID) and isinstance(miad[val], str) and (iid := allaids.get(miad[val].lower())): mokens[s][e][val]=iid
	

def keyify(mokens: list[list], gid: int = DEFGID) -> list:
	allstrs=K
	if not KEYS.get(gid): KEYS[gid]={}
	for q, a in KEYS[gid].items(): allstrs.setdefault(a, q)

	lookups={}

	for s, mexps in enumerate(mokens):
		for e, miad in enumerate(mexps):
			for col, val in CV:
				if OPERS[miad[OPER]][col] in (RID,AID) and isinstance(miad[val], int) and miad[val] not in allstrs: lookups[miad[val]]=1
		
	if lookups:
		rows=db.selectin({'bid':lookups.keys(), 'rid':[I['key']], 'gid':[gid]}, DB['table_meme'])
		for row in rows: KEYS[int(row[oGID])][row[oALP]] = int(row[oBID])
		for q, a in KEYS[gid].items(): allstrs.setdefault(a, q)

	for s, mexps in enumerate(mokens):
		for e, miad in enumerate(mexps):
			for col, val in CV:
				if OPERS[miad[OPER]][col] in (RID,AID) and isinstance(miad[val], int):
					if miad[val] in allstrs: mokens[s][e][val]=allstrs[miad[val]]


# Run decode() and identify()
def idecode(memestr: str, gid: int = DEFGID) -> list:
	mokens = decode(memestr)
	identify(mokens, gid)
	return mokens


# Run keyify() and encode()
def keyencode(mokens: list[list], gid: int = DEFGID) -> str:
	keyify(mokens, gid)
	return encode(mokens)


###############################################################################
#						 MEMELANG -> SQL QUERIES
###############################################################################

def selectify(mexps: list[list], gid: int = DEFGID) -> tuple[str, list]:

	n	     = 0
	bb	     = [n]
	aa	     = [n]
	gid	     = str(int(gid))
	where 	 = f"n{n}.gid={gid}"
	groupby  = f"n{n}.gid,n{n}.bid"
	join	 = ''
	select   = f"""';{{' || n{n}.gid || ':' || n{n}.bid"""
	params 	 = []
	lastcol2   = None

	for miad in mexps:

		col1, link, oeql1, oeql2 = OPERS[miad[OPER]][COL1], OPERS[miad[OPER]][LINK], OPERS[miad[OPER]][EQL1], OPERS[miad[OPER]][EQL2]
		col2 = OPERS[miad[OPER]][COL2] or AID

		if link == LBEG:
			join += f' FROM {DB['table_meme']} n{n}'
			lastcol2 = col2

		elif link == LAND:
			n+=1
			bb.append(n)
			join += f" LEFT JOIN {DB['table_meme']} n{bb[-1]} ON n{n}.gid={gid} AND n{aa[-1]}.bid=n{bb[-1]}.bid"
			if col2 == AID and lastcol2 == AID:
				join += f" AND (n{aa[-1]}.aid!=n{bb[-1]}.aid OR n{aa[-1]}.rid!=n{bb[-1]}.rid)"

		elif link == LFWD:
			n+=1
			aa.append(n)
			bb.append(n)
			select	 += f""", ' {{' || n{n}.gid || ':' || n{n}.bid"""
			join	 += f" JOIN {DB['table_meme']} n{n} ON n{bb[-2]}.aid=n{n}.aid AND n{n}.gid={gid} AND n{bb[-2]}.bid!=n{n}.bid"
			groupby  += f", n{n}.gid, n{n}.bid"
			lastcol2 = col2

		elif link == LREV:
			select	+= ", '}'"
			aa.pop()
			continue

		if miad[VAL1] is not None:
			where += f" AND n{n}.{col1}{oeql1}%s"
			params.append(miad[VAL1])

		select += f''', string_agg(DISTINCT CONCAT(' ', n{n}.rid, '=', CONCAT(n{n}.{AMT}, 'a', n{n}.{AID}, '"',  n{n}.{ALP}, '"')), '')'''

		if miad[VAL2] is not None:
			if col2 == ALP:
				where += f" AND LOWER(n{n}.{col2}) LIKE %s"
				params.append(miad[VAL2].lower())
			else:
				where += f" AND n{n}.{col2}{oeql2}%s"
				params.append(miad[VAL2])

	return f"SELECT CONCAT({select}) AS raq {join} WHERE {where} GROUP BY {groupby}", params


def sqlify(mokens: list[list], gid: int = DEFGID) -> tuple[str, list]:
	selects, params = [], []
	for s, mexps in enumerate(mokens):
		qry_select, qry_params = selectify(mexps, gid)
		selects.append(qry_select)
		params.extend(qry_params)
	return ' UNION '.join(selects), params


def put (mokens: list[list], gid: int) -> list[list]:
	
	if not gid: raise Exception('put gid')
	if gid not in KEYS: KEYS[gid]={}

	rows = []
	params = []

	# NEW KEY NAMES

	newkeys = {}
	l2u = {}

	for s, mexps in enumerate(mokens):
		for e, miad in enumerate(mexps):
			if OPERS[miad[OPER]][COL1] == BID and e!=0: raise Exception('bid must be first')
			for col, val in CV:
				if miad[val] is None: raise Exception(f'Invalid null {miad}')
				if isinstance(miad[val], str):
					if iid := KEYS[gid].get(miad[val]): mokens[s][e][val]=iid
					elif re.search(r'[^a-zA-Z0-9]', miad[val]): raise Exception(f'Invalid key {miad[val]} in {miad}')
					else: 
						alp = db.slugify(miad[val])
						alpl = alp.lower()
						if not newkeys.get(alpl):
							newkeys[alpl] = 0
							l2u[alpl] = alp

	if newkeys:
		# Unique check keys
		krows=db.selectin({'gid':[gid], 'rid':[I['key']], f'LOWER({ALP})':newkeys.keys()}, DB['table_meme'])
		for row in krows:
			alp=row[oALP]
			alpl = alp.lower()
			if newkeys.get(alpl):
				if int(row[oBID]) == int(newkeys[alpl]) or newkeys[alpl] is None: newkeys.pop(alpl, 0)
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
			rows.append(PLACEHOLDER)
			params.extend([gid, aid, I['key'], None, None, alp])

		# Swap missing keys for new IDs
		identify(mokens, gid)
	
	# NEW MEMES
	for s, mexps in enumerate(mokens):
		for e, miad in enumerate(mexps):
			if e==0:
				if OPERS[miad[OPER]][COL1] == BID:
					bid=miad[VAL1]
					continue
				else: bid = db.seqinc(DB['table_seqn'])

			aid, amt, alp = None, None, None

			if OPERS[miad[OPER]][COL2] == AID: aid = miad[VAL2]
			elif OPERS[miad[OPER]][COL2] == AMT: amt = miad[VAL2]
			elif OPERS[miad[OPER]][COL2] == ALP: alp = miad[VAL2]
			else: raise Exception('put col')

			rows.append(PLACEHOLDER)
			params.extend([gid, bid, miad[VAL1], aid, amt, alp])

	sqls=f"INSERT INTO {DB['table_meme']} VALUES " + ','.join(rows) + " ON CONFLICT DO NOTHING"
	db.insert(sqls, params)

	return mokens


def get(mokens: list[list], gid: int = DEFGID) -> list[list]:
	sql, params = sqlify(mokens, gid)
	res = db.select(sql, params)

	if not res: return []

	responsestr=''
	for row in res: responsestr+=row[0]
	responsestr = re.sub(r'(\d)a""', r'\1', responsestr).replace('""', '')
	#print(responsestr)
	return decode(responsestr)


# Input: Memelang query string
# Output: Integer count of resulting memes
def count(mokens: list[list], gid: int = DEFGID) -> int:
	sql, params = sqlify(mokens, gid)
	return len(db.select(sql, params))


def deljob(j: str, mokens: list[list], gid: int = DEFGID) -> int:
	if not gid: raise Exception('deljob gid')

	fields = {}
	values = {AID:None, RID:None, BID:None}
	col2 = AID
	sqls, params = [], []

	if len(mokens)!=1: raise Exception('deljob moken count')

	for miad in mokens[0]:
		if miad[VAL1]:
			col1 = OPERS[miad[OPER]][COL1]
			if col1 in (RID,BID):
				if values[col1]: raise Exception('deljob double value')
				values[col1]=miad[VAL1]
			else: raise Exception('deljob col1')

		if miad[VAL2]:
			if values[AID] is not None: raise Exception('deljob double aid value')
			col2 = OPERS[miad[OPER]][COL2]
			values[AID]=miad[VAL2]

	if j == 'delg':
		fields = {AID:False, RID:False, BID:False}
		sqls.append(f"DELETE FROM {DB['table_meme']} WHERE gid=%s")
		params.append([gid])

	elif j == 'dela':	
		fields = {AID:True, RID:False, BID:False}
		sqls.append(f"DELETE FROM {DB['table_meme']} WHERE gid=%s AND aid=%s")
		params.append([gid, values[AID]])

	elif j == 'delr':
		fields = {AID:False, RID:True, BID:False}
		sqls.append(f"DELETE FROM {DB['table_meme']} WHERE gid=%s AND rid=%s")
		params.append([gid, values[RID]])
		
	elif j == 'delb':
		fields = {AID:False, RID:False, BID:True}
		sqls.append(f"DELETE FROM {DB['table_meme']} WHERE gid=%s AND bid=%s")
		params.append([gid, values[BID]])

	elif j == 'delarb':
		fields = {AID:True, RID:True, BID:True}
		sqls.append(f"DELETE FROM {DB['table_meme']} WHERE gid=%s AND {col2}=%s AND rid=%s AND bid=%s")
		params.append([gid, values[AID], values[RID], values[BID]])
	
	else: raise Exception(f'deljob unknown')

	for k in fields:
		if fields[k] == True:
			if not values[k]: raise Exception(f"deljob field {k} is empty")
		elif fields[k] == False:
			if values[k]: raise Exception(f"deljob field {k} must be empty")

	db.inserts(sqls, params)
	return 1


def jobify(memestr: str, morekeys: list = []) -> dict:
	found=False
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
		elif key == 'g':
			try: val = int(val)
			except: raise Exception(f'int err {key}')

		job[key] = val
		job['*'] = True

	return job


# Input: Memelang query string
# Output: Memelang results string
def query(memestr: str = None) -> str:
	mokens = decode(memestr);

	if not mokens: return ''

	job=jobify(memestr)
	if job['*']: mokens.pop(0)
	if not job['j']: job['j'] = 'get'
	if not job['g']: job['g'] = DEFGID

	if job['j'] == 'put': return keyencode(put(mokens, job['g']), job['g'])
	else:
		identify(mokens, job['g'])
		if job['j'] == 'get': return keyencode(get(mokens, job['g']), job['g'])
		elif job['j'] == 'cnt': return 'amt=' + str(count(mokens, job['g']))
		elif job['j'].startswith('del'): return 'amt=' + str(deljob(job['j'], mokens, job['g']))
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
	mokens = decode(memestr)
	print ("TOKENS:", mokens)
	print ("QUERY:", encode(mokens).replace('\n', ';'))

	mokens = idecode(memestr)
	sql, params = sqlify(mokens)
	full_sql = db.morfigy(sql, params)
	print(f"SQL: {full_sql}\n")

	# Execute query
	print(f"RESULTS:")
	print(query(memestr))
	print()
	print()

def cli_put(memestr: str):
	print(keyencode(put(decode(memestr), DEFGID), DEFGID))
	print()
	print()

# Read a meme file and save it to DB
def cli_putfile(file_path):
	with open(file_path, 'r', encoding='utf-8') as f: print(keyencode(put(decode(f.read()), DEFGID), DEFGID))
	

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
		mokens=decode(memestr)
		print('Tokens:', mokens)
		identify(mokens)
		print('Idents:', mokens)
		print('Query 1:', memestr)
		mokens2 = copy.deepcopy(mokens)
		memestr2 = memestr

		for i in range(2,4):
			memestr2 = keyencode(mokens2).replace("\n", ";")
			mokens2 = idecode(memestr2)
			print(f'Query {i}:', memestr2)


		sql, params = sqlify(mokens)
		print('SQL: ', db.morfigy(sql, params))
		
		c1=count(mokens)
		c2=count(mokens2)
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
		f"CREATE TABLE {DB['table_meme']} (gid BIGINT, bid BIGINT, rid BIGINT, aid BIGINT, amt DOUBLE PRECISION, alp VARCHAR(511), PRIMARY KEY (gid,bid,rid));",
		f"CREATE INDEX {DB['table_meme']}_rid_idx ON {DB['table_meme']} (rid);",
		f"CREATE INDEX {DB['table_meme']}_aid_idx ON {DB['table_meme']} (aid) WHERE aid IS NOT NULL;",
		f"CREATE INDEX {DB['table_meme']}_amt_idx ON {DB['table_meme']} (amt) WHERE amt IS NOT NULL;",
		f"CREATE INDEX {DB['table_meme']}_alp_idx ON {DB['table_meme']} (aalp) WHERE alp IS NOT NULL;",
		f"GRANT USAGE, UPDATE ON SEQUENCE {DB['table_seqn']} TO {DB['user']};",
		f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE {DB['table_meme']} TO {DB['user']};",
	]

	for command in commands: db.psql(command)


# Delete database table
def cli_tabledel():
	commands = [
		f"DROP SEQUENCE {DB['table_seqn']};",
		f"DROP TABLE IF EXISTS {DB['table_meme']};",
	]
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
