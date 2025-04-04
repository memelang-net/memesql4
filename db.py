import os, re
from psycopg2.pool import ThreadedConnectionPool

DB = {
	'host': 'localhost',
	'user': 'memeuser',
	'pass': 'memepswd',
	'name': 'memedb',
}

GLOBAL_POOL = None

def poolinit():
	global GLOBAL_POOL
	if GLOBAL_POOL is None:
		GLOBAL_POOL = ThreadedConnectionPool(
			minconn=1,
			maxconn=5,
			host=DB['host'],
			database=DB['name'],
			user=DB['user'],
			password=DB['pass']
		)
	return GLOBAL_POOL

def select(sql: str, params: list = None) -> list:
	if params is None: params = []
	pool = poolinit()
	conn = pool.getconn()
	try:
		with conn.cursor() as cursor:
			cursor.execute(sql, params)
			rows = cursor.fetchall()
		return [list(row) for row in rows]
	finally:
		pool.putconn(conn)

def insert(sql: str, params: list = None):
	if params is None: params = []
	pool = poolinit()
	conn = pool.getconn()
	try:
		with conn.cursor() as cursor:
			cursor.execute(sql, params)
			conn.commit()
	except Exception as e:
		conn.rollback()
		raise e
	finally:
		pool.putconn(conn)

def inserts(sqls: list[str], params: list[list]):
	pool = poolinit()
	conn = pool.getconn()

	if len(sqls) != len(params): raise ValueError("sql/paramter mismatch in db.inserts")

	try:
		with conn.cursor() as cursor:
			for sql, param in zip(sqls, params):
				if sql: cursor.execute(sql, param)
			conn.commit()
	except Exception as e:
		conn.rollback()
		raise e
	finally:
		pool.putconn(conn)

def inreturn(sql: str, params: list = None):
	if params is None: params = []
	pool = poolinit()
	conn = pool.getconn()
	try:
		with conn.cursor() as cursor:
			cursor.execute(sql, params)
			conn.commit()
			row = cursor.fetchone()
			return row[0] if row else None
	except Exception as e:
		conn.rollback()
		raise e
	finally:
		pool.putconn(conn)


def selectin(cols: dict = {}, table: str = None) -> list:
	if not table: raise ValueError("No table provided.")

	conds, params = [], []
	for col in cols:
		conds.append(f"{col} IN (" + ','.join(['%s'] * len(cols[col])) + ")")
		params.extend(cols[col])

	if not conds:
		return []

	pool = poolinit()
	conn = pool.getconn()
	try:
		with conn.cursor() as cursor:
			cursor.execute(f"SELECT DISTINCT * FROM {table} WHERE " + ' AND '.join(conds), params)
			rows = cursor.fetchall()
		return [list(row) for row in rows]
	finally:
		pool.putconn(conn)

def seqinc(seqn: str = None) -> int:
	if not seqn: raise ValueError("No sequence name provided.")
	pool = poolinit()
	conn = pool.getconn()
	try:
		with conn.cursor() as cursor:
			cursor.execute(f"SELECT nextval('{seqn}')")
			inc = int(cursor.fetchone()[0])
			conn.commit()
		return inc
	except Exception as e:
		conn.rollback()
		raise e
	finally:
		pool.putconn(conn)

def psql(sql: str, db: str = None):
	if not db: raise ValueError("No db name provided.")
	command = f"sudo -u postgres psql -d {db} -c \"{sql}\""
	print(command)
	os.system(command)

def morfigy(sql: str, params: list) -> str:
	for param in params:
		rep = param.replace("'", "''") if isinstance(param, str) else str(param)
		sql = sql.replace("%s", rep, 1)
	return sql

# Input: "John Adams"
# Output: "JohnAdams"
def slugify(string: str) -> str:
	return re.sub(r'[^a-zA-Z0-9]', '', string)
