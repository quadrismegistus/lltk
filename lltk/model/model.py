class BaseModel(object):
	def __getattr__(self,key):
		if key.startswith('path_') and hasattr(self,'paths') and getattr(self,'paths') is not None:
			return self.paths.get(key[5:])
		try:
			return self.__getattribute__(key)
		except AttributeError:
			return None
			# try:
			# if hasattr(self,'text') and self.text is not None:
			# 	return self.text.__getattr__(key)