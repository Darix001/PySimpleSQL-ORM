from array import array
from itertools import repeat
from functools import partial
from pydrycode.cachetools import AttrCache, LiteralCache

RNONE = repeat(None)
RSQLQUERY = repeat('sqlquery')

literal = LiteralCache()

@AttrCache
def dbstring(string, /):
	return string.replace('_', SPACE)

SPACE = chr(32)# 


OP_P = chr(40) #(

CLOSE_P = chr(41)#)

COMMA = chr(44) + SPACE#, 

LINE = chr(10)#\n

TAB = chr(9)#\t

OPEN_DBO_STRING = f"{OP_P}{LINE}{TAB}"

CLOSE_DBO_STRING = LINE + CLOSE_P#;

END_DBO_STRING = chr(59) + LINE

COLSEP = f"{COMMA[:1]}{LINE}{TAB}"

seps = dict.fromkeys(('where', 'having'), dbstring._and_)

literal.BEGIN += END_DBO_STRING

literal.COMMIT += END_DBO_STRING

EMPTY = TAB[0:0]


class array_context:
	__slots__ = 'write'

	def __init__(self, array, /):
		self.write = getattr(array, self.writer)

	def __enter__(self, /):
		self.write(self.opener)

	def __exit__(self, exc_type, exc_val, exc_tb, /):
		self.write(self.closer)


class arraygroup(array_context):
	__slots__ = ()
	writer = 'append'
	opener = OP_P
	closer = CLOSE_P


class array_transaction(array_context):
	writer = 'fromunicode'		
	opener = literal.BEGIN
	closer = literal.COMMIT


class array(array):
	__slots__ = 'params'
	__lshift__ = write = array.fromunicode

	def print_(self, args, /, sep=SPACE):
		self << sep.join(args)

	def write_args(self, args, /):
		self.append(OP_P)
		print(*args, sep=COMMA, end=CLOSE_P, file=self)

	def sqlquery(self, obj, /):
		if sqlquery := getattr(obj, 'sqlquery', None):
			sqlquery(self)
		else:
			self.add_unknown(obj)

	def add_unknown(self, obj, /):
		self.append(QMARK)
		self.params.append(obj)

	def fromstmts(self, stmts:dict, /):
		for stmt, args in stmts.items():
			self << stmt
			self.append(SPACE)
			self.sqlquery_args(args, seps.get(stmt, COMMA))

	def sqlquery_args(self, /, sep=COMMA):
		first = True
		for sqlquery in map(getattr, args, RSQLQUERY, RNONE):
			if not first:
				self << sep
			else:
				first = not first
			if sqlquery:
				sqlquery(self)
			else:
				self.add_unknown(obj)

	def addspace(self, /):
		self.append(SPACE)

	def pad(self, string, char=SPACE, /):
		self << f"{char}{string}{char}"

	encloser = property(arraygroup)

	transaction = property(array_transaction)


dbarray = partial(array, 'u')