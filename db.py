from array import array
from copy import deepcopy
from functools import partial
from operator import attrgetter
from sqlite3 import connect, Connection
from consts import dbarray, END_DBO_STRING, dbstring


class Base:
	__slots__ = ()

	def __enter__(self, /):
		return self


class Connection(Connection, Base):
	__slots__ = ()

	def __exit__(self, exc_type, exc_val, exc_tb, /):
		if exc_type:
			self.interrupt()
		else:
			self.commit()
		self.close()

	def create(self, /, *args, safe=True):
		array = dbarray()
		
		with array.transaction:
				
			for obj in args:
				data = obj.Meta
				array << CREATE
				
				if kind := data.kind:
					array.pad(kind)
				else:
					array.addspace()
				
				array << type(obj).__name__

				if safe:
					array << dbstring._if_not_exists_

				array << data.fullname
				data.sqldef(array)
				array << END_DBO_STRING
		
		self.executescript(array.tounicode())
		return array


def set_connect(path, func_name='connect'):
	global connect
	import importlib
	module = importlib.import_module(path)
	connect = getattr(module, func_name)


CREATE = Connection.create.__name__

connect = partial(connect, factory=Connection)