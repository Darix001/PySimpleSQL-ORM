from consts import *
from sys import path
from array import array
from copy import deepcopy
from sqlite3 import connect
from operator import attrgetter


get_Meta = attrgetter('Meta')

path.append(r'C:\Users\elmen\OneDrive\Documentos\Python\pydrycode files')


def funcname_to_default(func, /):
	func.__defaults__ = func.__name__,
	return func


class array(array):
	__slots__ = ()
	__lshift__ = array.fromunicode

	def enclose(self, string, /):
		self << f"{OP_P}{string}{CLOSE_P}"

	@funcname_to_default
	def create(self, obj, string, /):
		self << f"{string}{SPACE}{(kind := obj.kind)}"
		if kind:
			self.append(SPACE)


class Base:
	__slots__ = ()

	def __enter__(self, /):
		return self


class Query(Base):
	__slots__ = ('cursor', 'array', 'params')

	def __init__(self, con, string='', params=(), /):
		self.con = con
		self.array = array(string)
		self.params = [*params]

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
		cls = type(self)
		return cls(self.con, self.array, self.params)

	def deepcopy(self, memo=None, /):
		cls = type(self)
		return cls(self.con, self.array, deepcopy(self.params, memo))

	__copy__ = copy

	def new_line(self, string, params, /):
		self.array << string
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
		data = array('u')
		for table in tables:
			table.Meta.sqldef(data)
		self.con.executescript(data.tounicode())


def set_connect(path, func_name='connect'):
	global connect
	import importlib
	module = importlib.import_module(path)
	connect = getattr(module, func_name)


del path