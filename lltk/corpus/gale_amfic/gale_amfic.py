from lltk.imports import *


################
## 1. Text class
################

attr2xml={'psmid': 'PSMID',
 'asset_id': 'assetID',
 'asset_id_e_toc': 'assetIDeTOC',
 'dvi_collection_id': 'dviCollectionID',
 'bibliographic_id': 'bibliographicID',
 'reel': 'reel',
 'mcode': 'mcode',
 'ocr': 'ocr',
 'pub_date_start': 'pubDate pubDateStart',
 'release_date': 'releaseDate',
 'source_library_name': 'sourceLibrary libraryName',
 'source_library_location': 'sourceLibrary libraryLocation',
 'language': 'language',
 'document_type': 'documentType',
 'notes': 'notes',
 'comments': 'comments',
 'author_composed': 'author composed',
 'author_first': 'author first',
 'author_middle': 'author middle',
 'author_last': 'author last',
 'author_birth_date': 'author birthDate',
 'author_death_date': 'author deathDate',
 'full_title': 'fullTitle',
 'display_title': 'displayTitle',
 'imprint_full': 'imprintFull',
 'imprint_publisher': 'imprintPublisher',
 'collation': 'collation',
 'publication_place_city': 'publicationPlaceCity',
 'publication_place_state': 'publicationPlaceState',
 'publication_place_country': 'publicationPlaceCountry',
 'publication_place_composed': 'publicationPlaceComposed',
 'total_pages': 'totalPages'}


class TextGaleAmericanFiction(Text):

    def get_meta_from_file(self):
        """
        This function mines the XML of the file for metadata, returning it as a dictionary.
        Please make sure to include the id of the text!
        """

        # create empty dictionary
        metadata_dict={}

        # store the id
        metadata_dict['id']=self.id

        # pull out the xml using BeautifulSoup
        import bs4
        header = self.text_xml.split('<text>')[0]
        soup = bs4.BeautifulSoup(header,'lxml')

        # loop over attribute,selector in above dictionary
        for attr,selector in attr2xml.items():
            # get the tag in xml
            tag = soup.select_one(selector)

            # get a string version
            tagstr=(' '.join(tag.stripped_strings) or '') if tag else ''

            # assign it to the dictionary
            metadata_dict[attr]=tagstr

        # return dictionary
        return metadata_dict


    def text_plain(self, force_xml=None):
        """
        This function returns the plain text file.
        - If it can find a plain text copy, it returns that.
        - Otherwise, it will convert the XML to plain text.

        Since this XML format is identical to Gale's ECCO XML,
        we're using a function created there to parse our XML (self.dom = ).
        """

        # Return plain text version if it exists
        if self.exists_txt: return self.text_plain_from_txt()

        # Otherwise, load from XML:
        from lltk.corpus.ecco import gale_xml2txt     # identical to ECCO's reader, adapted there
        return gale_xml2txt(self.xml_soup(),correct_ocr=True)  # pass the words through Ted's OCR Corrector dictionary





##################
## 2. Corpus class
##################



class GaleAmericanFiction(Corpus):
    TEXT_CLASS=TextGaleAmericanFiction

    def load_metadata(self):
        import numpy as np
        meta=super().load_metadata(clean=False)
        meta['genre']='Fiction'
        if meta.index.name!='id' and 'id' not in set(meta.columns):
            meta['id']=meta['psmid']
        meta['year']=[int(str(x)[:4]) if str(x)[:4].isdigit() else np.nan for x in meta.pub_date_start]
        meta['title']=meta['full_title']
        meta['author']=meta['author_composed']
        meta=clean_meta(meta)
        return meta