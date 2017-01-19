from flask import Flask
from flask_script import Manager, Shell
from models.futures import Series, Contract, Daily, Base

app = Flask(__name__)
app.debug = False

manager = Manager(app)

def _make_context():
	return {'Base': Base, 'Series': Series}

if __name__ == "__main__":
	manager.add_command('shell', Shell(make_context=_make_context))
	manager.run()