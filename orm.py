from consts import *
from itertools import chain
from functools import partial
from connection import Database
from pydrycode.attrtools import Slots, Slot
from pydrycode.cachetools import AttrCache, LiteralCache

convert = repr


class Base(Slots, defaults=(None,) * 2):
	__slots__ = ('name', 'args')

	def __str__(self, /):
		return self.name

	def __set_name__(self, cls, name, /):
		if not (sqlname := self.name):
			self.name = sqlname = name
		
		elif sqlname in cls.__dict__:
			setattr(cls, name, self := self._replace(name=name))

		getattr(cls.Meta, self.listname).append(self)


	def join_map(self, sep=COMMA, /):
		return sep.join(map(func, self.args))


class DBO(Base, defaults=(None,)*3):
	__slots__ = 'kind'
	
	def add_kind(self, array, /):
		if kind := self.kind:
			return array << kind
			array.append(SPACE)


class Field(DBO):
	__slots__ = ()
	listname = 'args'

	def __get__(self, instance, owner=None):
		if instance is None:
			return FieldProxy(self, owner)
		elif constraints := self.constraints:
			return constraints.default

	def sqldef(self, array, /) -> str:
		array << self.name
		
		if kind := self.kind:
			array.append(' ')
			kind.sqldef(array)

		if args := self.args:
			args.sqldef(array, self.name)


class FieldProxy(Slots):
	__slots__ = ('field', 'cls')
	
	def __str__(self, /):
		return f"{self.cls}.{self.field}"

	def __getattr__(self, attr, /):
		return getattr(self.field, attr)


class Dtype(Base, defaults=(None,) * 4):
	__slots__ = ('adapter', 'converter')

	def __call__(self, /, *args):
		return self._replace(args=args)

	def __set_name__(self, cls, name, /):
		setattr(cls, name, field := Field(name=name, kind=self))
		field.__set_name__(cls, name)

	def __or__(self, constraints, /):
		return Field(args=constraints, kind=self)

	def sqldef(self, array, /):
		array << self.name
		if args := self.args:
			array << repr(args)
			if len(args) == 1:
				del array[-2]


class Constraints(Slots, repr=True,
	defaults=(None, False, True, None, None, None, None)):
	__slots__ = ('primary_key', 'unique', 'null', 'check', 'default',
		'collate', 'foreign_key')

	def __set_name__(self, cls, name, /):
		setattr(cls, name, field := Field(name=name, args=self))
		field.__set_name__(cls, name)

	def __ror__(self, value, /):
		return Field(kind=Dtype(value.__name__), args=self)

	__or__ = __ror__

	def sqldef(self, array, field_name='', /):
		if (default := self.default) is not None:
			array << dbstring._default_
			array << convert(default)
		
		if self.primary_key:
			array << dbstring._primary_key
		
		elif self.unique:
			array << dbstring._unique

		if not self.null:
			array << dbstring._not_null

		if check := self.check:
			check = check.literal().format(field_name)
			array << f"{literal.check}{SPACE}{check}{SPACE}"

		if collate := self.collate:
			array << f'{literal.collate}{SPACE}{collate}'

		if fk := self.foreign_key:
			array << f"{SPACE}{literal.references}{SPACE}{fk.owner.Meta.name}"
			array.enclose(fk.field.name)


class Metadata(DBO, repr=True):
	__slots__ = ('constraints', 'schema', 'indexes', 'safe', 'wr', 'fullname')

	def __set_name__(self, cls, name, /):
		if not self.name:
			self.name = cls.__name__

	def sqldef(self, array, /):
		array.create(self)
		array << dbstring.TABLE_

		if self.safe:
			array << dbstring.IF_NOT_EXISTS_

		array << f"{self.fullname}{OP_P}"
		
		
		for obj in chain(self.args, self.constraints):
			obj.sqldef(array)
			array.append(COMMA)
		
		array[-1] = CLOSE_P

		if self.wr:
			array << dbstring._WITHOUT_ROWID

		array.append(P_COMMA)

		for index in self.indexes:
			index.sqldef(array)

	def add(self, /, *args):
		for obj in args:
			getattr(self, obj.listname).append(self)


class Meta(type):
	__slots__ = ()

	def __prepare__(name, bases, /, tblname=None, wr=False, kind='',
		schema='', safe=None):
		if not tblname:
			tblname = name
		Meta = Metadata(
			tblname,
			[], #args | fields
			kind,
			[], #constraints
			schema,
			[], #indexes
			safe,
			wr,
			schema + tblname,
			)
		return {'Meta':Meta}

	def __str__(self, /):
		return self.Meta.fullname
		

class Table(metaclass=Meta):
	__slots__ = 'additional_data'
	rowid = Dtype("Integer") | Constraints(primary_key=True)

	def __init__(self, /, **data):
		keys = vars(cls := type(self)).keys()
		kw = data.keys()
		if keys >= kw:
			for k, v in data.items():
				setattr(self, k, v)
		else:
			kw = ', '.join(kw - keys)
			raise TypeError(
				f"Invalid(s) Keyword(s) Argument(s) For {cls}: {kw}")


class Constraint(DBO):
	__slots__ = ()
	listname = 'constraints'
	
	def __init__(self, kind, /, *args, name=''):
		super().__init__(name, args, kind)

	def sqldef(self, array, /):
		if name := self.name:
			array << f"{self.__class__.__name__}{SPACE}{name}{SPACE}"
		array << self.kind
		array.enclose(self.join_map())


class Index(DBO):
	__slots__ = ()
	listname = 'indexes'

	def __init__(self, /, *args, name='', kind=''):
		super().__init__(name, kind, args)

	def sqldef(self, array, schema, /):
		array.create(self)
		array << f"{literal.INDEX}{SPACE}{schema}{self.name}"
		array.enclose(self.join_map())


@AttrCache
def fn(string, /):
	def function(*args):
		pass
	return function


@AttrCache
def dbstring(string, /):
	return string.replace('_', ' ')


dtypes = AttrCache(Dtype)


def from_dataclass(cls, text=dtypes.Text):
	namespace = cls.__dict__
	args = cls.Meta.args
	for k, v in cls.__annotations__.items():
		if k in namespace:
			c = Cs(default=namespace[k])
		else:
			c = Cs(null=False)
		if v is str:
			v = text
		else:
			v = getattr(dtypes, v.__name__)
		args.append(field := Field(name=k, args=c, kind=v))
		setattr(cls, k, field)
	return cls


def main():
	with Database(':memory:') as db:
		@from_dataclass
		class Test(Table):
			ID:int
			name:str
			lastname:str
			salary:float=.0
	
		db.create(Test)
	
	print(Test(ID=1))


schema = LiteralCache()

literal = LiteralCache()

Cs = Constraints

C = Constraint

c = AttrCache(partial(partial, Constraint))


if __name__ == '__main__':
	main()