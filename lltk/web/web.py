# from gevent import monkey,sleep
# monkey.patch_all()

import os,sys

path_self = os.path.realpath(__file__)
path_code = os.path.abspath(os.path.join(path_self,'..','..','..'))
path_code_data = os.path.join(path_code,'data')
path_fields = os.path.join(path_code_data,'fields')
path_stopwords = os.path.join(path_code_data,'stopwords.txt')
os.environ['PYTHONPATH']=f'{path_code}:'+os.environ.get('PYTHONPATH','')
sys.path.insert(0,path_code)


MODERNIZE_SPELLING=True

import pickle,json,time,gzip
from collections import defaultdict,Counter
import numpy as np
from flask import Flask
from flask import render_template, current_app, g, jsonify, request
import lltk,os
from lltk import tools
import pandas as pd
from collections import defaultdict,Counter,namedtuple
from pymongo import MongoClient
import ujson as json
from mongo_datatables import DataTables
from flask_pymongo import PyMongo
# from flask_socketio import SocketIO,emit
from flask_session import Session



from pymongo import MongoClient
client = MongoClient()
db_lltk = client['lltk']





ONLY_FIELDS={'VG.Human','VG.Object','VG.Animal','W2V.Locke.Abstract','W2V.Locke.Concrete','W2V.HGI.Abstract','W2V.HGI.Concrete'}

ONLY_FIELDS={'CP.speech_words','CP.poetry_words','CP.prose_words','CP.poetic_diction_notspeech','CP.poetic_diction',
			'REF.content_words_nj','REF.content_words','REF.byu','REF.english'}

ONLY_FIELDS|={'CH.Abstract.1500-1524', 'CH.Abstract.1525-1549', 'CH.Abstract.1550-1574', 'CH.Abstract.1575-1599', 'CH.Abstract.1600-1624', 'CH.Abstract.1625-1649', 'CH.Abstract.1650-1674', 'CH.Abstract.1675-1699', 'CH.Abstract.1700-1724', 'CH.Abstract.1725-1749', 'CH.Abstract.1750-1774', 'CH.Abstract.1775-1799', 'CH.Abstract.1800-1824', 'CH.Abstract.1825-1849', 'CH.Abstract.1850-1874', 'CH.Abstract.1875-1899', 'CH.Abstract.1900-1924', 'CH.Abstract.1925-1949', 'CH.Abstract.1950-1974', 'CH.Abstract.1975-1999', 'CH.Abstract.All', 'CH.Abstract.Robust', 'CH.Concrete.1500-1524', 'CH.Concrete.1525-1549', 'CH.Concrete.1550-1574', 'CH.Concrete.1575-1599', 'CH.Concrete.1600-1624', 'CH.Concrete.1625-1649', 'CH.Concrete.1650-1674', 'CH.Concrete.1675-1699', 'CH.Concrete.1700-1724', 'CH.Concrete.1725-1749', 'CH.Concrete.1750-1774', 'CH.Concrete.1775-1799', 'CH.Concrete.1800-1824', 'CH.Concrete.1825-1849', 'CH.Concrete.1850-1874', 'CH.Concrete.1875-1899', 'CH.Concrete.1900-1924', 'CH.Concrete.1925-1949', 'CH.Concrete.1950-1974', 'CH.Concrete.1975-1999', 'CH.Concrete.All', 'CH.Concrete.Robust', 'CH.Neither.All', 'CH.Neither.Robust'}

ONLY_FIELDS|={'CH.Abstractness.0.0','CH.Abstractness.0.1', 'CH.Abstractness.0.2', 'CH.Abstractness.0.3', 'CH.Abstractness.0.4', 'CH.Abstractness.0.5', 'CH.Abstractness.0.6', 'CH.Abstractness.0.7', 'CH.Abstractness.0.8', 'CH.Abstractness.0.9', 'CH.Concreteness.0.0', 'CH.Concreteness.0.1', 'CH.Concreteness.0.2', 'CH.Concreteness.0.3', 'CH.Concreteness.0.4', 'CH.Concreteness.0.5', 'CH.Concreteness.0.6', 'CH.Concreteness.0.7', 'CH.Concreteness.0.8', 'CH.Concreteness.0.9'}
#ONLY_FIELDS|={'CH.Abstractness2.0.0','CH.Abstractness2.0.1', 'CH.Abstractness2.0.2', 'CH.Abstractness2.0.3', 'CH.Abstractness2.0.4', 'CH.Abstractness2.0.5', 'CH.Abstractness2.0.6', 'CH.Abstractness2.0.7', 'CH.Abstractness2.0.8', 'CH.Abstractness2.0.9', 'CH.Concreteness2.0.0', 'CH.Concreteness2.0.1', 'CH.Concreteness2.0.2', 'CH.Concreteness2.0.3', 'CH.Concreteness2.0.4', 'CH.Concreteness2.0.5', 'CH.Concreteness2.0.6', 'CH.Concreteness2.0.7', 'CH.Concreteness2.0.8', 'CH.Concreteness2.0.9'}

ONLY_FIELDS|={'CH.Abstractness.Consolidated.0.0','CH.Abstractness.Consolidated.0.1', 'CH.Abstractness.Consolidated.0.2', 'CH.Abstractness.Consolidated.0.3', 'CH.Abstractness.Consolidated.0.4', 'CH.Abstractness.Consolidated.0.5', 'CH.Abstractness.Consolidated.0.6', 'CH.Abstractness.Consolidated.0.7', 'CH.Abstractness.Consolidated.0.8', 'CH.Abstractness.Consolidated.0.9', 'CH.Concreteness.Consolidated.0.0', 'CH.Concreteness.Consolidated.0.1', 'CH.Concreteness.Consolidated.0.2', 'CH.Concreteness.Consolidated.0.3', 'CH.Concreteness.Consolidated.0.4', 'CH.Concreteness.Consolidated.0.5', 'CH.Concreteness.Consolidated.0.6', 'CH.Concreteness.Consolidated.0.7', 'CH.Concreteness.Consolidated.0.8', 'CH.Concreteness.Consolidated.0.9'}



ONLY_FIELDS|={'W2V.Abstractness.0.0','W2V.Abstractness.0.1', 'W2V.Abstractness.0.2', 'W2V.Abstractness.0.3', 'W2V.Abstractness.0.4', 'W2V.Abstractness.0.5', 'W2V.Abstractness.0.6', 'W2V.Abstractness.0.7', 'W2V.Abstractness.0.8', 'W2V.Abstractness.0.9', 'W2V.Concreteness.0.0', 'W2V.Concreteness.0.1', 'W2V.Concreteness.0.2', 'W2V.Concreteness.0.3', 'W2V.Concreteness.0.4', 'W2V.Concreteness.0.5', 'W2V.Concreteness.0.6', 'W2V.Concreteness.0.7', 'W2V.Concreteness.0.8', 'W2V.Concreteness.0.9'}
ONLY_FIELDS|={'CP.poetic_diction_notspeech'}

ONLY_FIELDS|=set(['CP.poetic_diction_notspeech','CP.poetic_diction_notspeech_eng',
'CP.poetic_diction','CP.poetic_diction_eng',
'CP.poetic_diction_mdw','CP.poetic_diction_mdw_eng',
'CP.poetic_diction_mdw_speech','CP.poetic_diction_mdw_prose',
'CP.poetic_diction_mdw_speech_NOT','CP.poetic_diction_mdw_prose_NOT',
'W2V.HGI.Abstract','W2V.HGI.Concrete','W2V.HGI.Neither',
'W2V.Locke.Abstract','W2V.Locke.Concrete','W2V.Locke.Neither',
'CH.Abstract.Robust','CH.Concrete.Robust','CH.Neither.Robust',
'CH.Abstract.All','CH.Concrete.All','CH.Neither.All',
'W2V.Consolidated.Abstract','W2V.Consolidated.Concrete','W2V.Consolidated.Neither',
'W2V.Consolidated.Abstract_eng','W2V.Consolidated.Concrete_eng','W2V.Consolidated.Neither_eng',
'W2V.Consolidated.Abstract_nj','W2V.Consolidated.Concrete_nj','W2V.Consolidated.Neither_nj',
'W2V.Consolidated.Abstract_njv','W2V.Consolidated.Concrete_njv','W2V.Consolidated.Neither_njv'])

#print(sorted(list(ONLY_FIELDS)))

#def create_app():

# create app
app = Flask(__name__)
app.config["MONGO_URI"] = "mongodb://local:27017/lltk"
mongo = PyMongo(app)
sess = Session(app)
# socketio = SocketIO(app, ping_timeout=3600, async_mode='gevent', manage_session=False)


@app.before_request
def before_request():
	g.lltk=lltk
	g.sorted=sorted


@app.route("/")
def index():
	return render_template('index.html')

@app.route('/api_csv/<path:fn>')
def api_csv(fn,basic=True,corpus_name=None):
	if not os.path.exists(fn): fn='/'+fn
	corpus_name=request.args.get('corpus_name')
	data = {}
	header=None
	rows=[]
	for dx in lltk.tools.read_ld(fn):
		if basic:
			title='<a href="/text/{corpus_name}/{text_id}">{text_title}</a>'.format(corpus_name=corpus_name,text_id=dx['id'],text_title=dx['title'])
			row = [dx['author'], title, dx['year']]
		else:
			row = [dx.get(k,'') for k in header]
		rows+=[row]
	data['data'] = rows
	return jsonify(data)

@app.route('/corpus_json/<corpus_name>')
def corpus_json(corpus_name, keys=['id','author','title_link','year']):
	# from lltk.tools.db import get_corpus_meta
	print('>> querying')
	# corpus_meta=get_corpus_meta(corpus_name)
	corpus_meta = lltk.corpus(corpus_name).meta
	print('>> packaging')
	rows=[]
	for dx in corpus_meta:
		dx['title_link']='<a href="/text/{corpus_name}/{text_id}">{text_title}</a>'.format(corpus_name=corpus_name,text_id=dx['id'],text_title=dx['title'])
		row=[dx.get(h,'') for h in keys]
		#print(row)
		rows+=[row]
	data = {'data':rows}
	print('>> done packaging data')
	return jsonify(data)

# @TODO GET THIS MONGO TABLE TO WORK!
#
# @app.route('/mongo/<corpus_name>/<collection>')
# def api_db(corpus_name, collection):
# 	request_args = json.loads(request.values.get("args"))
#
# 	custom_filter={'corpus':corpus_name}
# 	results = DataTables(mongo, collection, request_args, **custom_filter).get_rows()
#
# 	print '>>',results
# 	return json.dumps(results)


@app.route('/corpus/<corpus_name>')
def corpus(corpus_name):
	corpus_obj=lltk.load_corpus(corpus_name)
	#csv_url='/api_csv/'+corpus_obj.path_metadata[1:]
	return render_template('corpus2.html',corpus_name=corpus_name,corpus_obj=corpus_obj)

@app.route('/tokens')
def tokens():
	return render_template('tokens.html')



# def load_fields1():
# 	## HEAVY LIFTING??
# 	print('>> getting word2fields')
# 	import time
# 	now=time.time()
# 	#word2fields=tools.get_word2fields(only_fields=ONLY_FIELDS)

# 	import pickle,json
# 	from collections import defaultdict
# 	word2fields=defaultdict(set)
# 	# with open('../abstraction/words/data.all_fields.pickle','rb') as f:
# 	# with open('data/data.fields.pickle','rb') as f:
# 		# fields = pickle.load(f)
# 	with open(os.path.expanduser('~/github/AbsLitHist/data/fields/data.vecfields.AbsConc.json')) as f:
# 		fields = json.load(f)
# 	# fields={}
# 	# for field_fn in os.listdir(path_fields):
# 	# 	if not field_fn.endswith('.txt'): continue
# 	# 	field_name=field_fn[:-4]
# 	# 	field_fnfn=os.path.join(path_fields,field_fn)
# 	# 	with open(field_fnfn) as f:
# 	# 		field_data=set(f.read().strip().split('\n'))
# 	# 		fields[field_name]=field_data
	

# 	for field in sorted(fields):
# 		if 1: #field in ONLY_FIELDS:
# 			for word in fields[field]:
# 				word2fields[word]|={field}

# 	print('done.',time.time()-now)
# 	field2words=fields
# 	return (field2words,word2fields)


# def load_fields():
# 	## HEAVY LIFTING??
# 	print('>> getting word2fields')
# 	now=time.time()

# 	word2fields=defaultdict(set)
# 	with gzip.open(os.path.expanduser('~/github/AbsLitHist/data/fields/data.vecfields.json.gz')) as f:
# 		fields = json.load(f)
# 	for field in sorted(fields):
# 		#if 1: #field in ONLY_FIELDS:
# 		if field.startswith('Abs-Conc.ALL.'):
# 			for word in fields[field]:
# 				word2fields[word]|={field}
# 	print('done.',time.time()-now)
# 	field2words=fields
# 	return (field2words,word2fields)

def load_fields():
	import sys
	#@HACK!!!
	sys.path.append('/home/ryan/github/abslithist')
	from abslithist.words import get_fields

	word2fields=defaultdict(set)
	fields=get_fields(fieldprefix='Abs-Conc.ALL',fieldsuffix=None)
	for fld in fields:
		for w in fields[fld]:
			word2fields[w]|={fld}
	return (fields,word2fields)




def save_fields(word2fields):
	import pickle
	ofn='cache.fields.%s.pickle' % tools.hash(str(sorted(list(ONLY_FIELDS))))
	print(ofn)





### PAGE
def show_text(text_obj, corpus_obj=None, meta={}, merge_line_breaks=False,modernize_spelling=MODERNIZE_SPELLING,show_top_words=250):
	from lltk.tools.freqs import get_word2fields,get_fields,measure_fields
	from pprint import pprint

	if not meta: meta=text_obj.meta
	print('META -->',meta)

	field2words,word2fields=load_fields()
	# pprint(field2words.keys())

	#save_fields(word2fields)

	#import lltk
	#english = lltk.load_english()
	#stopwords = lltk.load_stopwords()
	#stopwords|={'would','yet','also'}
	stopwords={w.strip() for w in open(path_stopwords).read().split() if w.strip() and not w.strip().startswith('#')}

	

	for w in stopwords:
		#print(w,'?')
		if w in word2fields:
			del word2fields[w]
	# only_pos=['n','j','v']
	only_pos={'n','j'}
	# only_pos={'n','j'}
	# only_pos={'n'}
	# only_words=set([w for pos in only_pos for w in pos2words[pos]])
	# print('only words ->',len(only_words))

	## add non-content?
	for pos in pos2words:
		print(pos)
		if pos in only_pos: continue
		for w in pos2words[pos]:
			stopwords|={w}

	# html,field_counts=text_obj.html(word2fields=word2fields,lim_words=None,only_words=only_words,modernize_spelling=modernize_spelling)
	html,field_counts=text_obj.html(word2fields=word2fields,lim_words=None,only_words=None,modernize_spelling=modernize_spelling,stopwords=stopwords)
	#html=html.replace('</br>','</br></br>')

	try:
		period_int=int(meta.get('author_dob',0)) + 30
		period_int = period_int // 25 * 25
		period = '{0}-{1}'.format(period_int, period_int+24)
	except (ValueError,AttributeError) as e:
		period = 'Unknown'

	corpus_name = corpus_obj.name if hasattr(corpus_obj,'name') else 'UnknownCorpus'

	from collections import OrderedDict
	data=OrderedDict()

	print(sorted(field_counts.keys()))
	fieldprefix='Abs-Conc.ALL'

	for period in ['C17','C18','C19','C20','_median']:
		sum1=sum(field_counts[f'{fieldprefix}.Abs.{period}'].values())
		sum2=sum(field_counts[f'{fieldprefix}.Conc.{period}'].values())
		sum3=sum(field_counts[f'{fieldprefix}.Neither.{period}'].values())
		data[f'Abs/Conc ({period})']=sum1/sum2 * 10

		if period in {'_median'}:
		# if period in {'C17','_median'}:
			data[f'Top {show_top_words} abstractions ({period})'] = ', '.join(f'{w} ({c})' for w,c in field_counts[f'{fieldprefix}.Abs.{period}'].most_common(show_top_words))
			data[f'Top {show_top_words} concretions ({period})'] = ', '.join(f'{w} ({c})' for w,c in field_counts[f'{fieldprefix}.Conc.{period}'].most_common(show_top_words))
			data[f'Top {show_top_words} neutrals ({period})'] = ', '.join(f'{w} ({c})' for w,c in field_counts[f'{fieldprefix}.Neither.{period}'].most_common(show_top_words))
	
	### Other data

	# data['Abs/Conc']=field_counts['AbsConc.Abs'] / field_counts['AbsConc.Conc'] * 10
	# data['Abs/Conc_C17']=field_counts['AbsConc.Abs_C17'] / field_counts['AbsConc.Conc_C17'] * 10
	# data['Abs/Conc_C18']=field_counts['AbsConc.Abs_C18'] / field_counts['AbsConc.Conc_C18'] * 10
	# data['Abs/Conc_C19']=field_counts['AbsConc.Abs_C19'] / field_counts['AbsConc.Conc_C19'] * 10
	# data['Abs/Conc_C20']=field_counts['AbsConc.Abs_C20'] / field_counts['AbsConc.Conc_C20'] * 10

	# print('data',data)

	#data['Abstractness Ratio (Consolidated)']=field_counts['W2V.Consolidated.Abstract_njv'] / field_counts['W2V.Consolidated.Concrete_njv'] if field_counts['W2V.Consolidated.Concrete_njv'] else 0
	# data['Abstract words / 10 Concrete words (robust)']=field_counts['CH.Abstract.Robust'] / field_counts['CH.Concrete.Robust'] * 10 if field_counts['CH.Concrete.Robust'] else 0
	# data['Abstract words / 10 Concrete words (consolidated, njv)']=field_counts['W2V.Consolidated.Abstract_njv'] / field_counts['W2V.Consolidated.Concrete_njv'] * 10 if field_counts['W2V.Consolidated.Concrete_njv'] else 0
	# #data['Abs/Conc*10 (robust)']=data['Abstract/Concrete*10 (robust)']
	# data['Abstract words / 10 Concrete words (orig)']=field_counts['W2V.HGI.Abstract'] / field_counts['W2V.HGI.Concrete'] * 10 if field_counts['W2V.HGI.Concrete'] else 0
	# data['Abstract words / 10 Concrete words (all)']=field_counts['CH.Abstract.All'] / field_counts['CH.Concrete.All'] * 10 if field_counts['CH.Concrete.All'] else 0
	# data['Abstract words / 10 Concrete words (local)']=field_counts['CH.Abstract.%s' % period] / field_counts['CH.Concrete.%s' % period] * 10 if field_counts['CH.Concrete.%s' % period] else 0
	# to_median=data.keys()
	# data['Abstract words / 10 Concrete words (MEDIAN)']=round(np.median([data[x] for x in to_median if data[x]]),1)

	# data['Post-Norman words / 10 pre-Norman words']=field_counts['CP.postnorman_words'] / field_counts['CP.prenorman_words']*10 if field_counts['CP.prenorman_words'] else ''


	# data['# Words (content)']=field_counts['REF.content_words']
	# data['# Words (english)']=field_counts['REF.english']
	# data['# Abstract words (Consolidated, njv)']=field_counts['W2V.Consolidated.Abstract_njv']
	# data['# Concrete words (Consolidated, njv)']=field_counts['W2V.Consolidated.Concrete_njv']
	# data['# Neutral words (Consolidated, njv)']=field_counts['W2V.Consolidated.Neither_njv']

	# data['% Poetic Diction (V1)'] = field_counts['CP.poetic_diction'] / field_counts['REF.english'] * 100 if field_counts['REF.english'] else ''
	# data['% Poetic Diction (V2)'] = field_counts['CP.poetic_diction_notspeech'] / field_counts['REF.english'] * 100 if field_counts['REF.english'] else ''
	# #data['% Poetic Diction (V3)'] = field_counts['CP.poetic_diction_mdw'] / (field_counts['CP.poetic_diction_mdw']+field_counts['CP.MDW.NotPoetic']) * 100 if (field_counts['CP.MDW.Poetic']+field_counts['CP.MDW.NotPoetic']) else ''
	# data['% Poetic Diction (V3b)'] = field_counts['CP.poetic_diction_mdw_eng'] / field_counts['REF.english'] * 100 if field_counts['REF.english'] else ''

	for k,v in data.items():
		if type(v)==float:
			data[k]=round(v,1)

	if merge_line_breaks: html=html.replace('<br/><br/>','<br/>')

	return render_template('text3.html',
							corpus_name=corpus_name, corpus_obj=corpus_obj,
							text_obj=text_obj, text_meta=meta, text_html=html,
							data=data, period=period)


# @app.route('/send', methods=['GET', 'POST'])
# def send():
#     if request.method == 'POST':
#         kapa = request.form['age']
#         age = make_nil(int(kapa))
#         sys.stdout('age: ', age)
#         return render_template('age.html', age=age)
#     return render_template('inka.html')

@app.route('/text/custom',methods=['GET','POST'])
def text_custom(modernize_spelling=MODERNIZE_SPELLING):
	if request.method == 'POST':
		txt=request.form.get('txt')
		if txt:
			if modernize_spelling:
				from lltk.tools import get_spelling_modernizer,modernize_spelling_in_txt
				spelling_d=get_spelling_modernizer()
				txt=modernize_spelling_in_txt(txt,spelling_d)

			text_obj = lltk.text.PlainText(txt=txt)
			return show_text(text_obj)
	return show_text_box()

def show_text_box_orig():
	return render_template('text_box.html')


def get_absconc_wordclasses(field2words):
	import pickle
	with open('/home/ryan/github/lltk/data/fields/data.abs_conc_levels.pickle','rb') as f:
		field_levels=pickle.load(f)
	res=[]
	for i,fname in enumerate(field_levels):
		fwords=field_levels[fname]
		for fword in fwords:
			res.append((fword,fname))
	return res


def show_text_box(word2level={},text_default='',corpus=None,text_id=None,meta={}):
	global wclasses,field2words,stopwords

	# return render_template('text_box.html')
	if not word2level:
		word2level=dict(
			[
				(w,i)
				for w,i in wclasses
				if w not in stopwords and w.isalpha()
				#and w in field2words['REF.content_words'] and len(w)>3
			]
		)

	return render_template('edit_xml.html',
		word2level=word2level,
		text_default=text_default,
		corpus=corpus,
		text_id=text_id,
		meta=meta
	)


# Dickens/Bleak_House/Z200035096/015
@app.route('/edit/<corpus_name>/<path:text_id>')
def edit_text(corpus_name,text_id):
	# corpus_obj=lltk.load_corpus(corpus_name)
	#text_obj=corpus_obj.TEXT_CLASS(text_id,corpus_obj)

	#from lltk.tools.db import get_text_meta
	#meta=get_text_meta(corpus_name,text_id)
	import lltk

	path_texts=os.path.join(lltk.config['PATH_TO_CORPORA'],corpus_name,'texts')
	meta_path = os.path.join(path_texts,text_id,'meta.json')
	meta=json.load(open(meta_path))

	
	text_path = os.path.join(path_texts,text_id,'text.xml')
	text_anno_path = os.path.join(path_texts,text_id,'text.anno.xml')
	text_anno_path_bak = os.path.join(path_texts,text_id,'.text.anno.xml.bak')

	if os.path.exists(text_anno_path):
		with open(text_anno_path) as f:
			text_xml = f.read()
	elif os.path.exists(text_path):
		with open(text_path) as f, open(text_anno_path,'w') as of:
			text_xml = f.read()
			of.write(text_xml)
	else:
		text_xml=''

	return show_text_box(text_default=text_xml,corpus=corpus_name,text_id=text_id,meta=meta)

@app.route('/edit2/<corpus_name>/<path:text_id>')
def edit_text2(corpus_name,text_id):
	# corpus_obj=lltk.load_corpus(corpus_name)
	#text_obj=corpus_obj.TEXT_CLASS(text_id,corpus_obj)

	#from lltk.tools.db import get_text_meta
	#meta=get_text_meta(corpus_name,text_id)
	import lltk
	C=lltk.load(corpus_name)
	t=C.textd[text_id]
	text_path=t.path_txt
	print(text_path,os.path.exists(text_path))
	text_anno_path = text_path.replace('.xml','.anno.xml').replace('.txt','.anno.txt')
	text_anno_path_bak = text_path.replace('.xml','.anno.xml.bak').replace('.txt','.anno.txt.bak')
	meta=t.meta

	if os.path.exists(text_anno_path):
		with open(text_anno_path) as f:
			text_xml = f.read()
	elif os.path.exists(text_path):
		with open(text_path) as f, open(text_anno_path,'w') as of:
			text_xml = f.read()
			of.write(text_xml)
	else:
		text_xml=''

	return show_text_box(text_default=text_xml,corpus=corpus_name,text_id=text_id,meta=meta)

@app.route('/edit/custom')
def edit_text_custom():
	return show_text_box()



@app.route('/save/<corpus_name>/<path:text_id>',methods=['POST'])
def save_text(corpus_name,text_id):
	import lltk
	path_texts=os.path.join(lltk.config['PATH_TO_CORPORA'],corpus_name,'texts')
	text_path = os.path.join(path_texts,text_id,'text.xml')
	text_anno_path = os.path.join(path_texts,text_id,'text.anno.xml')
	text_anno_path_bak = os.path.join(path_texts,text_id,'.text.anno.xml.bak')
	
	new_txt=request.form.get('text')
	if new_txt:

		if os.path.exists(text_anno_path):
			with open(text_anno_path) as f:
				old_txt=f.read()
				
			if old_txt!=new_txt:
				with open(text_anno_path_bak,'w') as of:
					of.write(old_txt)
					print('>> saved:',text_anno_path_bak)
		
				with open(text_anno_path,'w') as of:
					of.write(new_txt)
					print('>> saved:',text_anno_path)
	
	return jsonify(success=True)



@app.route('/text/<corpus_name>/<path:text_id>')
def text(corpus_name,text_id):
	corpus_obj=lltk.load_corpus(corpus_name)
	text_obj=corpus_obj.TEXT_CLASS(text_id,corpus_obj)

	from lltk.tools.db import get_text_meta
	meta=get_text_meta(corpus_name,text_id)

	return show_text(text_obj, corpus_obj=corpus_obj, meta=meta, merge_line_breaks=True, modernize_spelling=MODERNIZE_SPELLING)



@app.route('/text_model/<corpus_name>/<path:text_id>')
def text_w_model(corpus_name,text_id):
	corpus_obj=lltk.load_corpus(corpus_name)
	text_obj=corpus_obj.TEXT_CLASS(text_id,corpus_obj)

	from lltk.tools.freqs import get_word2fields,get_fields,measure_fields
	word2fields=get_word2fields(only_fields=ONLY_FIELDS)
	#field2words=get_fields(only_fields=ONLY_FIELDS,word2fields=word2fields)
	#word2fields={}

	## add model results?
	fn_model_result=None
	fn_model_coeffs=None
	if corpus_name=='ChadwyckPoetry':
		#interval=text_obj.year_author_is_30 // 25 * 25
		#group_name=str(interval)+'-'+str(interval+24)
		#group_name=pd.read_csv('../agency/data.groups.ChadwyckPoetry.id2group.txt').set_index('id').loc[text_id].to_dict()['group']
		#group_name=tools.ld2dd(tools.read_ld('../agency/data.groups.ChadwyckPoetry.id2group.txt'),'id').get(text_id,{}).get('group','ungrouped')
		#fn_model_coeffs='../agency/annotations/models/%s/coeffs.txt' % group_name
		#fn_model_result='../agency/annotations/models/%s/results.txt' % group_name
		fn_model_coeffs=''
		fn_model_result='../agency/annotations/model_results_by_text_v3-1line/%s.txt' % text_id

		if not os.path.exists(fn_model_coeffs): fn_model_coeffs=None
		if not os.path.exists(fn_model_result): fn_model_result=None


	html=text_obj.html(word2fields=word2fields,fn_model_result=fn_model_result,fn_model_coeffs=fn_model_coeffs)

	# data
	data={}
	"""
	word_counts=text_obj.fast_counts()
	sumc=float(sum(word_counts.values()))
	field_counts=measure_fields(word_counts,only_fields=ONLY_FIELDS)
	fdf=pd.DataFrame([{'field':f, 'count':c} for f,c in field_counts.items() if c])
	if fdf is not None:
		fdf=fdf.sort_values('count',ascending=False).set_index('field')
		fdf['perc']=[round(c/sumc*100,2) for c in fdf['count']]
		data['Prevalence of key semantic fields']=fdf
	ratio1=field_counts['W2V.Locke.Abstract']/float(field_counts['W2V.Locke.Concrete'])*10 if field_counts['W2V.Locke.Concrete'] else 0
	ratio_ld=[{'ratio_type':'# Abstract Words per 10 Concrete Words', 'ratio':ratio1}]
	rdf=data['Indices of abstractness']=pd.DataFrame(ratio_ld).set_index('ratio_type')
	"""

	#print text_id, text_obj
	from lltk.tools.db import get_text_meta
	meta=get_text_meta(corpus_name,text_id)
	return render_template('text2.html',
							corpus_name=corpus_name, corpus_obj=corpus_obj,
							text_obj=text_obj, text_meta=meta, text_html=html,
							data=data)







### SOCKETS





def load_notebook(path,only_defs=False):
	import nbimporter
	nbimporter.options['only_defs'] = only_defs

	ppath,pfn = os.path.split(path)
	pname,pext = os.path.splitext(pfn)

	orig_path=os.getcwd()
	os.chdir(ppath)
	NBL = nbimporter.NotebookLoader()
	mod = NBL.load_module(pname)
	os.chdir(orig_path)
	return mod


def run():
	# socketio.run(app,port=1789,debug=False)
	app.run(host='0.0.0.0',port=1789,debug=0)






word2pos=json.load(open(os.path.join(path_code_data,'word2pos.json')))
pos2words=defaultdict(list)
for word,pos in word2pos.items(): pos2words[pos[:1]]+=[word]
# for word,pos in word2pos.items(): pos2words[pos]+=[word]

print(sorted(pos2words.keys()))


stopwords=set(open(path_stopwords).read().strip().split('\n'))

field2words,word2fields=load_fields()
wclasses=get_absconc_wordclasses(field2words)