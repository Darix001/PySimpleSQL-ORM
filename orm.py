from itertools import chain
from connection import Database, join_map
from pydrycode.attrtools import Slots


class Base(Slots, abstract=True):
	__slots__ = 'name'

	def __set_name__(self, cls, name, /):
		if not self.name:
			self.name = name


class Field(Base, defaults=('',)*2):
	__slots__ = ('dtype', 'constraints')

	def __get__(self, instance, owner=None):
		if instance is None:
			return FieldProxy(self, owner)
		elif constraints := self.constraints:
			return constraints.default

	def __str__(self, /):
		return f"{self.name} {self.dtype}{self.constraints}"

	def __set_name__(self, cls, name, /):
		super().__set_name__(cls, name)
		cls._fields_[name] = self


class FieldProxy(Slots):
	__slots__ = ('field', 'cls')
	
	def __str__(self, /):
		return f"{self.cls}.{self.field}"

	def __getattr__(self, attr, /):
		return getattr(self.field, attr)


class Dtype(Base, defaults=(None,)):
	__slots__ = 'values'

	def __call__(self, /, *values):
		if not self.values:
			return self._replace(values=values)
		raise ValueError("Type values are already specified for this type.")

	def __set_name__(self, cls, name, /):
		setattr(cls, name, field := Field(name, self))
		field.__set_name__(cls, name)

	def __or__(self, constraints, /):
		return Field(None, self, constraints)

	def __str__(self, /):
		name = self.name
		if values := self.values:
			name += f"({values[0]!r})" if len(values) == 1 else f"{values!r}"
		return name


class Constraints(Slots, repr=True,
	defaults=(None, False, True, None, None, None, None)):
	__slots__ = ('primary_key', 'unique', 'null', 'check', 'default',
		'collate', 'foreign_key')

	def __set_name__(self, cls, name, /):
		setattr(cls, name, field := Field(name, None, self))
		field.__set_name__(cls, name)

	def __ror__(self, value, /):
		return Field(None, Dtype(value.__name__), self)

	def __str__(self, string = '', /):
		if (default := self.default) is not None:
			string += f' default {default!r}'
		
		if self.primary_key:
			string += ' primary key'
		
		elif self.unique:
			string += ' unique'

		if not self.null:
			string += ' not null'

		if check := self.check:
			string += f' check {check}'

		if collate := self.collate:
			string += f' collate {collate}'

		if fk := self.foreign_key:
			table = fk.owner.Meta.name
			field = fk.field.name
			string += f'references {table}({field})'
		return string


class MetaData(Base, defaults=('', '', 'main.', (), (), None, False, ''),
	repr=True):
	__slots__ = ('ttype', 'schema', 'args', 'indexes',
		'info', 'wr', 'fullname')

	def __str__(self, /):
		args = join_map(',\n', self.args)
		wr = 'WITHOUT ROWID' if self.wr else ''
		indexes = join_map('\n'*2, self.indexes)
		return (
			f"CREATE {self.ttype} TABLE {self.fullname}"
			f"(\n{args}){wr};\n\n{indexes}")


Integer = Dtype("Integer")
Text = Dtype("Text")
Decimal = Dtype("Decimal")


class Meta(type):
	__slots__ = ()

	def __prepare__(name, bases, /):
		return {'_fields_':{}}

	def __str__(self, /):
		return self.Meta.fullname


class Table(metaclass=Meta):
	__slots__ = __args__ = ()
	rowid = Integer | Constraints(primary_key=True)

	def __init_subclass__(cls, /):
		if not (Meta := vars(cls).get('Meta')):
			cls.Meta = Meta = MetaData(cls.__name__)
		if Meta.without_rowid:
			cls.rowid = None
		if not Meta.fullname:
			Meta.fullname = Meta.schema + Meta.name
		Meta.args = [*cls._fields_.values(), *Meta.args]


C = Constraints


with Database(':memory:') as db:
	class Test(Table):
		ID = int | C(primary_key=True)
		name = Text
		salary = Decimal(10, 5)
	db.create(Test)

print(Test)