# install
# pip install bs4 fulltext epub-conversion pymupdf requests xml_cleaner html2text kitchen -q

# imports
import os
from kitchen.text.converters import to_unicode
# constants
WORKING_EXTS={'txt','pdf','epub','html','xml','htm'}
CONTENT_TAGS={'xml':['p'],'html':['p'],'htm':['p'],'epub':['p']}


#### Supporting epub code from https://github.com/soskek/bookcorpus/blob/master/epub2txt.py
# by @soskek
import os
import sys
import urllib.request, urllib.parse, urllib.error
import zipfile

import xml.parsers.expat
from glob import glob


class ContainerParser():
    def __init__(self,xmlcontent=None):
        self.rootfile = ""  
        self.xml = xmlcontent

    def startElement(self, name, attributes):
        if name == "rootfile": 
            self.buffer = ""    
            self.rootfile = attributes["full-path"]

    def parseContainer(self):
        parser = xml.parsers.expat.ParserCreate()
        parser.StartElementHandler = self.startElement
        parser.Parse(self.xml, 1)
        return self.rootfile

class BookParser():
    def __init__(self,xmlcontent=None):
        self.xml = xmlcontent 
        self.title = "" 
        self.author = "" 
        self.inTitle = 0
        self.inAuthor = 0
        self.ncx = ""

    def startElement(self, name, attributes):
        if name == "dc:title":
            self.buffer = ""
            self.inTitle = 1 
        elif name == "dc:creator":
            self.buffer = ""
            self.inAuthor = 1 
        elif name == "item":
            if attributes["id"] == "ncx" or attributes["id"] == "toc" or attributes["id"] == "ncxtoc":
                self.ncx = attributes["href"]

    def characters(self, data):
        if self.inTitle:
            self.buffer += data
        elif self.inAuthor:
            self.buffer += data

    def endElement(self, name):
        if name == "dc:title":
            self.inTitle = 0  
            self.title = self.buffer  
            self.buffer = ""
        elif name == "dc:creator":
            self.inAuthor = 0  
            self.author = self.buffer  
            self.buffer = ""

    def parseBook(self):
        parser = xml.parsers.expat.ParserCreate()
        parser.StartElementHandler = self.startElement
        parser.EndElementHandler = self.endElement
        parser.CharacterDataHandler  = self.characters
        parser.Parse(self.xml, 1)
        return self.title,self.author, self.ncx

class NavPoint():
    def __init__(self,id=None,playorder=None,level=0,content=None,text=None):
        self.id = id 
        self.content = content
        self.playorder = playorder
        self.level = level
        self.text = text

class TocParser():
    def __init__(self,xmlcontent=None):
        self.xml = xmlcontent 
        self.currentNP = None
        self.stack = []
        self.inText = 0
        self.toc = []

    def startElement(self, name, attributes):
        if name == "navPoint":
            level = len(self.stack)
            self.currentNP = NavPoint(attributes["id"], attributes["playOrder"], level)
            self.stack.append(self.currentNP)
            self.toc.append(self.currentNP) 
        elif name == "content":
            self.currentNP.content = urllib.parse.unquote(attributes["src"])
        elif name == "text":
            self.buffer = ""
            self.inText = 1

    def characters(self, data):
        if self.inText:
            self.buffer += data

    def endElement(self, name):
        if name == "navPoint":
            self.currentNP = self.stack.pop()
        elif name == "text":
            if self.inText and self.currentNP:
                self.currentNP.text = self.buffer
            self.inText = 0  

    def parseToc(self):
        parser = xml.parsers.expat.ParserCreate()
        parser.StartElementHandler = self.startElement
        parser.EndElementHandler = self.endElement
        parser.CharacterDataHandler  = self.characters
        parser.Parse(self.xml, 1)
        return self.toc

class Epub2Txt():
    def __init__(self,epubfile=None):
        self.epub = epubfile  

    def convert(self):
        file = zipfile.ZipFile(self.epub,"r");
        rootfile = ContainerParser(file.read("META-INF/container.xml")).parseContainer()
        title, author, ncx = BookParser(file.read(rootfile)).parseBook()
        ops = "/".join(rootfile.split("/")[:-1])
        if ops != "":
            ops = ops+"/"
        toc = TocParser(file.read(ops + ncx)).parseToc()
        texts=[]
            
        import html2text
        for t in toc:
            html = file.read(ops + t.content.split("#")[0])
            texts += [html2text.html2text(html.decode("utf-8"))]
        texts=[t.strip() for t in texts if type(t)==str and t.strip()]
        return '\n\n'.join(texts)

def epub2txt(epub_filename):
        return Epub2Txt(epub_filename).convert()
################################


###

# imports
import os

# constants
WORKING_EXTS={'txt','pdf','epub','html','xml','htm'}
CONTENT_TAGS={'xml':['p'],'html':['p'],'htm':['p'],'epub':['p']}

# Main function
def brute(fn_or_url):
    if fn_or_url.startswith('http:') or fn_or_url.startswith('https:'):
        return brute_url(fn_or_url)
    return brute_txt(fn_or_url)

# Convert xml to text via a set of content tags
def xml2txt(content,ctagnames):
    import bs4
    dom=bs4.BeautifulSoup(content)
    for ctagname in ctagnames:
        ctags=list(dom(ctagname))
        for ctag in ctags:
            ctag.name='newcontenttag'
    txt='\n\n'.join([ctag.text for ctag in dom('newcontenttag')])
    return txt

def pdf2txt(fn):
    import fitz
    with fitz.open(fn) as doc:
        pages=[page.getText().strip() for page in doc]
        pages=[p for p in pages if p]
        return '\n\n\n'.join(pages)
    return ''

def brute_txt(fn):
    """
    Convert anything to txt
    """
    # if url, send there
    if not os.path.exists(fn):
            print('! No filename found')
            return ''
    # get ext
    ext=os.path.splitext(fn)[-1][1:]
    txt=''
    # epub
    if ext in {'epub'}:
        txt=epub2txt(fn)
    elif ext in {'xml','html','htm'}:
        with open(fn) as f:
            content=f.read()
            txt=xml2txt(content,CONTENT_TAGS[ext])
    elif ext in {'txt'}:
        with open(fn,'rb') as f:
            content=f.read()
            return to_unicode(content)
    elif ext in {'pdf'}:
        txt=pdf2txt(fn)
    else:
        import fulltext
        txt=fulltext.get(fn)
        if not txt: return ''
    # clean
    txt=txt.replace('\xa0', ' ') 
    if 'project gutenberg ebook' in txt.lower():
        txt=clean_gutenberg(txt)
    return txt

def clean_gutenberg(txt):
    return txt.split('***END',1)[0].split('***START',1)[-1].split('***',1)[-1]


def brute_url(url):
    """
    Get a readable file from a variety of urls
    """
    # imports
    import os,tempfile
    import requests,secrets,urllib

    # can only do http/s
    if not url.startswith('http'): return None

    restypes={'text/plain', 'text/html'}

    # if straightforward
    r=requests.get(url)
    txt=''
    if r.status_code==200:
        _exts=[x for x in WORKING_EXTS if '.'+x in url]
        if not _exts: return 
        ext=_exts[0]
        with tempfile.NamedTemporaryFile(suffix='.'+ext) as of:
            of.write(r.content)
            txt=brute_txt(of.name)
    return txt

"""
e.g.
brute('http://www.gutenberg.org/files/1023/1023-h/1023-h.htm') -> Bleak House ...
brute('http://www.gutenberg.org/cache/epub/1023/pg1023.txt') --> Bleak House
brute('/home/ryan/Downloads/Outline.epub')
brute('/home/ryan/Downloads/Antimetricality.pdf')
# url='http://www.gutenberg.org/files/521/521-h/521-h.htm'
# url='http://www.gutenberg.org/cache/epub/1023/pg1023.txt'
# url='https://b-ok.cc/dl/4149249/be3e47'
url='https://www.dropbox.com/s/fu89fpcp2149aqi/Antimetricality-Poetics-Draft--2018-04-20.pdf?dl=1'
# url='https://www.dropbox.com/s/gcg3m97k8ms6wxq/Outline%20A%20Novel%20by%20Cusk%20Rachel%20%28z-lib.org%29%20%281%29.epub?dl=1'

"""

if __name__=='__main__':
    print(brute('http://gutenberg.net.au/ebooks02/0200991.txt'))
    # print(brute('http://www.gutenberg.org/cache/epub/1023/pg1023.txt')) # --> Bleak House
