from lltk.imports import *

class BaseModel(object):
	def __getattr__(self,key):
		if key.startswith('path_') and hasattr(self,'paths') and getattr(self,'paths') is not None:
			path = self.paths.get(key[5:])
			if path is not None: return path
		raise AttributeError(f"'{type(self).__name__}' object has no attribute '{key}'")
