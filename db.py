
from psycopg2.pool import ThreadedConnectionPool

# Database configurations
DB = {
	'host' : 'localhost',   # Host for Poseqres
	'user' : 'memeuser',    # Username for Poseqres
	'pass' : 'memepswd',    # Password for Poseqres
	'name' : 'memedb'      # Database name for Poseqres
}


pool = ThreadedConnectionPool(
	minconn=1,
	maxconn=5,
	host=DB['host'],
	database=DB['name'],
	user=DB['user'],
	password=DB['pass']
)

def select(sql: str, params: list = []) -> list:
	conn = pool.getconn()
	cursor = conn.cursor()
	cursor.execute(sql, params)
	rows=cursor.fetchall()
	pool.putconn(conn)
	return [list(row) for row in rows]


def insert(sql: str, params: list = []):
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

def inreturn(sql: str, params: list = []):
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
	if not table: table=DB['table_node']

	conds, params = [], []

	for col in cols:
		conds.append(f"{col} IN ("+ ','.join(['%s'] * len(cols[col])) +")")
		params.extend(cols[col])

	if not conds: return []

	conn = pool.getconn()
	cursor = conn.cursor()
	cursor.execute(f"SELECT DISTINCT * FROM {table} WHERE " + ' AND '.join(conds), params)
	rows=cursor.fetchall()
	pool.putconn(conn)
	return [list(row) for row in rows]


def seqinc(seqn: str = None) -> int:
	if not seqn: seqn=DB['table_seqn']
	conn = pool.getconn()
	try:
		with conn.cursor() as cursor:
			cursor.execute(f"SELECT nextval('{seqn}')")
			inc = int(cursor.fetchone()[0])
			conn.commit()
	except Exception as e:
		conn.rollback()
		raise e
	finally:
		pool.putconn(conn)

	return inc


def psql(sql: str, db: str = DB['name']):
	command = f"sudo -u postgres psql -d {db} -c \"{sql}\""
	print(command)
	os.system(command)


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