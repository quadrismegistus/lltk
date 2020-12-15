#encoding=utf-8
from __future__ import absolute_import
from __future__ import print_function
import os
from lltk.corpus import Corpus,Corpus_in_Sections
from lltk import tools
STYPE_FN=os.path.abspath(os.path.join(__file__,'../data.section_types.xlsx'))


import os,codecs
from lltk import tools
from lltk.text import Text,TextSection
from lltk.text import clean_text
import six


def xml2txt_save(self, **attrs):
    from lltk.text import xml2txt as x2t
    return x2t(OK=['p','l'], BAD=[], body_tag='text', **attrs)    


class TextSectionTCP(TextSection):
    DIVIDER_TAG='DIV1'

    def load_metadata(self):
        md=super(TextSectionTCP,self).load_metadata()
        md['genre'],md['medium']=self.parent.return_genre()
        return md

    @property
    def meta(self):
        md=super(TextSectionTCP,self).meta
        genre=self.corpus.parent.sectiontype2genre(self.type)
        if genre: md['genre']=genre
        return md

class TextTCP(Text):
    # split into sections
    def sections_xml(self,divider_tag='DIV1'):
        xml=self.xml
        o=[]
        for part in xml.split('<'+divider_tag)[1:]:
            part_type=part.split(' TYPE="')[1].split('"')[0]
            part='<'+divider_tag+part
            o+=[(part_type,part)]
        return o

    # xml -> txt
    def xml2txt(self, **attrs):
        return super().xml2txt(OK=['p','l'], BAD=[], body_tag='text', **attrs)

    def extract_metadata(self,mtxt):
        return extract_metadata(mtxt)
    
    def save_txt(self):
        super().save_txt(func=xml2txt)

    def return_genre(self,text_xml=None):
        speaker_tag=False
        l_tag=False
        p_tag=False

        txt=text_xml.upper() if text_xml else self.xml.upper()

        tags = ['</SPEAKER>', '</L>', '</P>']
        tag_counts={}
        for tag in tags: tag_counts[tag]=txt.count(tag)

        drama = tag_counts['</SPEAKER>'] > 100
        verse = tag_counts['</L>'] > tag_counts['</P>']
        prose = tag_counts['</L>'] <= tag_counts['</P>']


        medium = 'Verse' if verse else 'Prose'
        genre = 'Drama' if drama else medium

        return genre,medium

def extract_metadata(mtxt):
    md={}
    ## IDs
    for idnox in mtxt.split('</IDNO>')[1:-1]:
        idno_type=idnox.split('TYPE="')[1].split('">')[0]
        idno_id=idnox.split('">')[-1]

        md['id_'+idno_type.replace(' ','_')]=idno_id

    biblio = tools.yank(mtxt,'BIBLFULL')
    if biblio:
        md['title'] = biblio.split('</TITLE>')[0].split('">')[-1]
        md['author'] = tools.yank(biblio,'AUTHOR')
        md['extent'] = tools.yank(biblio,'EXTENT')
        md['pubplace'] = tools.yank(biblio,'PUBPLACE')
        md['publisher'] = tools.yank(biblio,'PUBLISHER')
        md['date'] = tools.yank(biblio,'DATE')
        md['notes']=tools.yank(biblio,'NOTESSTMT')

    try:
        md['year']=int(''.join([x for x in md.get('date','') if x.isdigit()][:4]))
    except (ValueError,TypeError) as e:
        md['year']=0

    return md



class TCP(Corpus):
    STYPE_DD=None

    def sectiontype2genre(self,stype):
        if not self.STYPE_DD:
            print('>> loading stype2genre')
            self.STYPE_DD=tools.ld2dd(tools.read_ld(STYPE_FN),'section_type')
        return self.STYPE_DD.get(stype,{}).get('genre','')

    #def __init__(self,**attrs):
    #	return super(TCP,self).__init__(**attrs)


def gen_section_types():
    import tools
    from lltk.corpus.eebo import EEBO_TCP
    from lltk.corpus.ecco import ECCO_TCP
    corpora = [EEBO_TCP(), ECCO_TCP()]

    from collections import defaultdict,Counter
    section_types=defaultdict(Counter)
    for c in corpora:
        cs=c.sections
        for d in cs.meta:
            section_types[c.name][d['section_type']]+=1

    def writegen():
        all_stypes = set([key for cname in section_types for key in section_types[cname]])
        for stype in all_stypes:
            dx={'section_type':stype}
            dx['count']=0
            for cname in section_types:
                dx['count_'+cname]=section_types[cname].get(stype,0)
                dx['count']+=dx['count_'+cname]
            yield dx

    tools.writegen('data.section_types.txt', writegen)
