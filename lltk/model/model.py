# -*- coding: utf-8 -*-

class Model(object):
	pass

class NullModel(Model):
	def __init__(self):
		pass

	@property
	def name(self):
		return ''
