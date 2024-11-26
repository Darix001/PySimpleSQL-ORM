from __future__ import annotations
import consts
from consts import dbstring, literal
import itertools as it
from functools import partial
from collections import abc
from pydrycode.attrtools import Slots
from operator import attrgetter, methodcaller
from sqlite3 import register_adapter, register_converter
from pydrycode.cachetools import Cache


convert = repr

dtypes = {'str':'text', 'bytes':'blob', 'bytearray':'blob'}

iterdump = methodcaller('iterdump')

code_names = attrgetter('__code__.co_names')


class DBO(Slots, add_repr=False, defs=('', None)):
	__slots__ = ('name', 'kind', 'c')

	def __str__(self, /):
		return self.name

class Field(DBO):
	__slots__ = ()
	listname = 'c'
	def __init__(self, name=None, kind=None, c=None, /, **kwargs):
		self.name = name
		self.kind = kind and getattr(kind, '__name__', kind)
		self.c = c or Constraints(**kwargs)

	def __set_name__(self, cls, attr, /):
		if self.c.primary_key:
			cls._pk = attr,
		if not (name := self.name):
			self.name = attr
		elif self.name in vars(cls):
			setattr(cls, attr, self._replace(name=attr))
		cls.Meta.c[attr] = self

	def __repr__(self, /):
		return f"<{super().__repr__()}>"

	def __get__(self, instance, owner=None):
		if instance is None:
			return FieldProxy(self, owner)
		elif (constraints := self.c) is not None:
			return constraints.default

	def iterdump(self, /) -> str:
		yield (name := self.name)
		if kind := self.kind:
			yield kind

		if constraints := self.c:
			yield from constraints.iterdump(name)


class FieldProxy(Slots):
	__slots__ = ('field', 'cls')

	def __str__(self, /):
		return f'{self.cls.Meta}."{self.field}"'

	def __getattr__(self, attr, /):
		return getattr(self.field, attr)

	def sqlquery(self, array, params, /):
		array << self.cls.Meta.fullname
		array.append(POINT)
		array.pad(self.field.name, QUOTE)


def register_type(cls, /, adapter=None, converter=None, typename=None):
	if typename is None:
		typename = cls.__name__
	if adapter is not None:
		register_adapter(cls, adapter)
	if converter is not None:
		register_converter(typename, converter)


defs = (False, False, True, None, None, None, None, None, None)

@code_names
def CONSTRAINTS_NAMES(self):
	(primary_key, unique, not_null, check, default, collate, references)


class Constraints(Slots, defs=defs):
	__slots__ = ('primary_key', 'unique', 'null', 'check', 'default',
		'collate', 'foreign_key', 'size', 'decimal_places')

	def __set_name__(self, cls, name):
		if v := cls.__annotations__.get(name):
			setattr(cls, name, Field(name, v, self))
		if self.primary_key:
			cls._pk = name,

	def iterdump(self, field_name='', /):
		if size := self.size:
			string = f"{consts.OP_P}{size!r}"
			if decimal_places := self.decimal_places:
				yield f"{string}{consts.COMMA}{decimal_places}{consts.CLOSE_P}"
			else:
				yield string +  consts.CLOSE_P

		selectors = (self.primary_key, self.unique, not self.null)
		yield from it.compress(CONSTRAINTS_NAMES, selectors)

		if check := self.check:
			yield CONSTRAINTS_NAMES[3]
			yield check.literal().format(field_name)

		if (default := self.default) is not None:
			yield CONSTRAINTS_NAMES[4]
			yield convert(default)

		if collate := self.collate:
			yield CONSTRAINTS_NAMES[5]
			yield collate

		if fk := self.foreign_key:
			yield CONSTRAINTS_NAMES[6]
			yield f"{fk.owner.Meta.name}{OP_P}{fk.field.name}{CLOSE_P}"

CONSTRAINTS_NAMES = [name.replace('_', consts.SPACE, 1)
for name in CONSTRAINTS_NAMES]



@code_names
def tabletypes(self):
	return (temp, virtual, strict)


class View(type):
	__slots__ = ()

	@classmethod
	def __prepare__(cls, name, bases, /, abstract=False, **kwargs):
		data = {}
		
		if not abstract:
			
			kwargs['name'] = name = kwargs.pop('dbname', name)
			schema = kwargs.setdefault('schema', consts.EMPTY)
			ttypes = iter(tabletypes)
			
			if kwargs.get('temp'):
				if not schema:
					kwargs['schema'] = schema = next(ttypes) + consts.POINT
			
			selectors = map(kwargs.pop, ttypes, consts.RNONE)
			kwargs['kind'] = consts.SPACE.join(it.compress(tabletypes, selectors))
			c = kwargs.setdefault('c', {})
			
			for base in bases:
				if Meta := getattr(base, 'Meta', None):
					c |= Meta.c

			if 'fullname' not in kwargs:
				kwargs['fullname'] = schema + name

			data['Meta'] = cls.Data(**kwargs)

		
		return data


	class Data(DBO, defs=(None,) * 3):
		__slots__ = ('schema', 'fullname', 'query')

		def __set_name__(self, cls, name, /):
			if annotations := cls.__annotations__:
				self.c = dict(zip(annotations,
					it.starmap(Field, annotations.items())))


		def sqldef(self, array, /):
			array << dbstring._As_
			self.query.literal(array)

		def add(self, /, *args):
			for obj in args:
				getattr(self, obj.listname).append(self)


class Table(View):
	__slots__ = ()
	
	def __set_name__(self, cls, name, /):
		setattr(cls, name, Field(name, foreign_key=self))


	class Data(View.Data, defs=(None, None, None, (), (), False)):
		__slots__ = ('constraints', 'indexes', 'wr')

		def __set_name__(self, cls, name, /):
			if annotations := cls.__annotations__:
				clsdata = vars(cls)
				c = self.c
				for k, v in annotations.items():
					if k in clsdata:
						if not isinstance(clsvalue := clsdata[k], Field):
							field = Field(k, v, default=clsvalue)
							setattr(cls, k, field)
						else:
							field = clsvalue
					else:
						field = Field(k, v, null=False)
						setattr(cls, k, field)
					c[k] = field

			if self.wr:
				cls.rowid = None

		def sqldef(self, array, /):
			array << consts.OPEN_DBO_STRING

			if (query := self.query) is not None:
				super().sqldef(array)
			else:
				c = self.c.values()
				columns_defs = map(consts.SPACE.join, map(iterdump, c))
				array.print_(columns_defs, consts.COLSEP)

				first = True
				for c in self.constraints:
					if not first:
						array << consts.COLSEP
					else:
						first = False
					c.sqldef(array)

				array << consts.CLOSE_DBO_STRING
			
				if self.wr:
					array << dbstring._WITHOUT_ROWID
	

class Base:
	__slots__ = ()
	def __init_subclass__(cls, /, abstract=False):
		super().__init_subclass__()
						

class BaseTable(Base, metaclass=Table, abstract=True):
	__slots__ = ()
	_pk = Meta = None
	rowid:int=Constraints(primary_key=True)
	
	def __init__(self, /, **kw):
		for k, v in kwargs.items():
			setattr(self, k, v)
				

class Constraint(DBO):
	__slots__ = ()
	listname = 'constraints'

	def sqldef(self, array, /):
		kind = self.kind
		if name := self.name:
			array.print_((__class__.__name__, name, kind))
		else:
			array << kind
		array.write_args(self.c)


class Index(DBO):
	__slots__ = ('tabledata', 'where')
	listname = 'indexes'

	def sqldef(self, array, /):
		array.print_((tabledata.schema, self.name, literal.On, tabledata.fullname))
		
		array.write_args(self.c)
		
		if where := self.where:
			where.literal(array)

			
@Cache
def c(string):
	string = strict.replace('_', consts.SPACE)
	def function(*args, name=None):
		return Constraint(name, args, string)
	return function





del Slots, defs