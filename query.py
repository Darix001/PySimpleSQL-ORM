from consts import SPACE, LINE, dbarray
from db import Base
from functools import partial
from orm import Base as OBase
from pydrycode.methodtools import set_name
from pydrycode.attrtools import Slots
from pydrycode.numtools import operators
from pydrycode.cachetools import AttrCache

# The & operator will have his binary effect, except on conditional objects. 
#In this cases, the & operator will be equivalent to the and operator.

class stmts:
	pass

class BaseSql(Slots):
	__slots__ = ()
	
	@set_name
	def operator(name, /):
		symbol = 0#some magic here
		return lambda self, other, /: Operator(self, symbol, other)

	@set_name
	def binary(name, /):
		name = name.replace('_', SPACE, 1)
		return lambda self, other, /: Operator(self, name, other)

	@set_name
	def conditional(name, /):
		name = name.strip('_')
		return lambda self, other, /: Operator(self, name, other)

	@set_name
	def suffix(name, /):
		return lambda self, /: Suffix(self, name)
	
	@set_name
	def preffix(name, /):
		return lambda self, /: Preffix(self, name)

	or_ = and_ = conditional

	like = regexp = match = is_ = is_not = glob = binary

	asc = desc = suffix

	distinct = preffix

	del preffix, binary, suffix, conditional, operator


class Suffix(BaseSql):
	__slots__ = ('obj', 'string')

	def sqlquery(self, array, /):
		self.obj.sqlquery(array)
		array.addspace()
		array << self.string


class Preffix(Suffix):
	__slots__  =()

	def sqlquery(self, array, /):
		array << self.string
		array.addspace()
		self.obj.sqlquery(array)
		

class Operator(BaseSql):
	__slots__ = ('left', 'symbol', 'rigth')

	def sqlquery(self, array, params, /):
		with array.encloser:
			array.sqlquery(self.left)
			array.pad(self.symbol)
			array.sqlquery(self.rigth)


class SQL(BaseSql):
	__slots__ = ('string', 'args')

	def __str__(self, /):
		return self.string

	def sqlquery(self, array, /):
		array << self.string
		array.params += self.params


class Row(BaseSql):
	__slots__ = ('args',)

	def __init__(self, /, *args):
		self.args = args

	def sqlquery(self, array, /):
		with array.encloser:
			array.sqlquery_args(self.args)


class Function(SQL):
	__slots__ = ()
	
	def __init__(self, name, /, *args):
		self.string = name
		self.args = Row(*args)
	
	def sqlquery(self, array, /):
		array << self.string
		self.args.sqlquery(array)


class Query(Base):
	__slots__ = ('con', 'stmts')

	def __init__(self, con, stmts={} /):
		self.con = con
		self.stmts = stmts()

	def __enter__(self):
		return self

	def __exit__(self, exc_type, exc_val, exc_tb, /):
		if exc_type is None:
			con = self.con
			try:
				self.execute()
			except con.DatabaseError:
				con.rollback()
			else:
				con.commit()
			finally:
				cursor.close()

	@set_name
	def select(name, /, default=(SQL(chr(42)),)):
		name = name.replace('_', SPACE, 1)
		def function(self, /, *args):
			setattr(self.stmts, name, args or default)
			return self
		return function

	group_by = where = having = order_by = select_distinct = selec

	def execute(self, /):
		array = dbarray()
		array.fromstmts(self.stmts)
		self.con.execute(array.tounicode(), array.params)


class Cte(OBase): #Common Table Expression
	__slots__ = ()


fn = AttrCache(partial(partial, Function))