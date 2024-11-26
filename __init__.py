__all__ = ['db', 'orm', 'consts', 'query']

from sys import path
path.append(r'C:\Users\elmen\OneDrive\Documentos\Python\pydrycode files')


from orm import BaseTable, Constraints
from db import connect


def main():
	with connect(':memory:') as db:
		class Test(BaseTable):
			ID:int
			name:str
			lastname:str
			salary:float=.0
	
		print(db.create(Test).tounicode())
	print(Test.name)
	# print(Test(1, 'Dariel', 'Buret'))


if __name__ == '__main__':
	main()