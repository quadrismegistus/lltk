from lltk.imports import *
from lltk.corpus.tcp import *


class TextEvansTCP(TextTCP):
    pass
#     def xml2txt(self,xml_fn):
#         """
#         Overwrite this if you have a custom function for parsing XML files to TXT files
#         """
#         # This is the default function 
#         txt = super().xml2txt(xml_fn)
#         return txt

# def xml2txt_evans_tcp(xml_txt_or_fn):
#     return xml2txt(xml_txt_or_fn)

class EvansTCP(TCP):
    TEXT_CLASS=TextEvansTCP
    COL_ID = 'id'
    XML2TXT = xml2txt_tcp

    def compile(self):
        """
        This is a custom installation function, downloading raw data
        and processing it into a form which minimally contains
        a metadata file stored at `metadata.csv` which has a column `self.COL_ID`
        """
        # This will download any file attached to `url_raw` on the corpus manifest
        # and unzip it to `path_raw` (usually ~/lltk_data/corpora/[corpusroot]/raw)
        self.compile_download(unzip=True)

        # Extract files
        self.compile_extract(extract_in=['XML_with_headers'])
        
        # Move the extracted XML files
        self.compile_texts(fn_startswith='',exts={'xml'},replacements={'.headed.':'.'})
        
        # Compile metadata from headers in the XML files
        self.compile_metadata()
        return
    
    

    def compile_extract(self, extract_in=['XML_with_headers']):
        # Defined in TCP class
        return super().compile_extract(extract_in=extract_in)


    def preprocess_txt(self,*args,**attrs):
        """
        Custom function here to produce txt files at `path_txt`
        from xml files at `path_xml`. By default this will map
        TextEvansTCP.xml2txt(xml_fn) over files at `path_xml`.
        """
        # By default will
        return super().preprocess_txt(*args,**attrs)

    def load_metadata(self,*args,**attrs):
        """
        Magic attribute loading metadata, and doing any last minute customizing
        """
        # This will save to `self.path_metadata`, set in the manifest path_metadata
        meta=super().load_metadata(*args,**attrs)
        # ?
        return meta

