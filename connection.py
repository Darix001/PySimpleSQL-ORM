from sys import path
from array import array
from copy import deepcopy
from sqlite3 import connect
from operator import attrgetter

get_Meta = attrgetter('Meta')

path.append(r'C:\Users\elmen\OneDrive\Documentos\Python\pydrycode files')


def join_map(sep, args, /):
	return sep.join(map(str, args))


class Base:
	__slots__ = ()

	def __enter__(self, /):
		return self


class Query(Base):
	__slots__ = ('cursor', 'array', 'params')

	def __init__(self, con, /):
		self.con = con
		self.array = array('u')
		self.params = []

	def __exit__(self, exc_type, exc_val, exc_tb, /):
		if exc_type is None:
			try:
				cursor = con.cursor()
				cursor.execute(self.array.tounicode(), self.params)
			except con.DatabaseError:
				con.rollback()
			else:
				con.commit()
			finally:
				cursor.close()

	def copy(self, /):
		new = Query(self.con)
		new.params += self.params
		new.array += self.array
		return new

	def deepcopy(self, memo={}, /):
		copy = self.copy()
		copy.params = deepcopy(copy.params)
		return copy

	__copy__ = copy

	def new_line(self, string, params, /):
		self.array.fromunicode(string)
		self.params += params
		

class Database(Base):
	__slots__ = 'con'

	def __init__(self, /, *args, **kwargs):
		self.con = connect(*args, **kwargs)

	def __exit__(self, exc_type, exc_val, exc_tb, /):
		con = self.con
		if exc_type:
			con.rollback()
		else:
			con.commit()
		con.close()

	def create(self, /, *tables):
		return (join_map('\n' * 3, map(get_Meta, tables)))


def set_connect(path, func_name='connect'):
	global connect
	import importlib
	module = importlib.import_module(path)
	connect = getattr(module, func_name)


del path