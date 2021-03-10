# ECCO Corpora

## ECCO-TCP

From its [website](https://www.textcreationpartnership.org/tcp-ecco/): "ECCO-TCP is a partnership with Gale, part of Cengage Learning, to produce highly accurate, fully-searchable, SGML/XML-encoded texts from among the 150,000 titles available in the Eighteenth Century Collections Online (ECCO) database."

ECCO-TCP is able to be shared freely. Here are links to versions of the metadata and data that I have adapted from the originals. Documentation on what was changed is also below.

### Download

* [Metadata](https://www.dropbox.com/s/9ex8rkfsbysc1oi/corpus-metadata.ECCO-TCP.xlsx?dl=0)
* [XML files](https://www.dropbox.com/s/yr6dz7clk4w5y2s/xml_eccotcp.zip?dl=0)
* [Plain text files](https://www.dropbox.com/s/8hjpbmtti5t02z8/txt_eccotcp.zip?dl=0)
* [Header metadata files](https://www.dropbox.com/s/8ej3a17puk941zh/headers_eccotcp.zip?dl=0)
* [XML schema description](https://www.dropbox.com/s/rwsvoee35pf61yw/schemas_eccotcp.zip?dl=0)
* [(All of the above in a single zip file [320MB])](https://www.dropbox.com/s/mrqof6muiodu67u/ECCO_TCP.metadata%2Bxml%2Btxt.zip?dl=0)

Or see the Dropbox folder [here](https://www.dropbox.com/sh/59xxy7fzg3va4ir/AABRcKXr4pndrLilXWgsSW1ha?dl=0).

### Metadata

I've extracted the metadata from the header metadata files, and saved it in an Excel file, along with custom columns, some of which are explained just below. The metadata file can be found [here](https://www.dropbox.com/s/9ex8rkfsbysc1oi/corpus-metadata.ECCO-TCP.xlsx?dl=0).

Non self-explanatory fields:
* **Genre**:
	* When available (in 2,188 of 2,387 cases), the genre of the text is taken from the metadata produced by Ted Underwood and Jordan Sellers in "The Emergence of Literary Diction" (*Journal of Digital Humanities*, 2012). Original data is [here]() (although currently a 404); my copy of it is [here](https://www.dropbox.com/s/ct1kf9p9sxjprqy/corpus-metadata.TedJDH.xls?dl=0), and [here](https://www.dropbox.com/s/a6k21lgew1pztby/matches.TedJDH--ECCO-TCP.xls?dl=0) is a list of the matches between the original ECCO-TCP and Underwood's and Sellers' corpus.
	* The remaining 199 texts were assigned a genre tag by Ryan Heuser, attempting to follow the general annotation schema of Underwood and Sellers.
	* Distribution:
		```
		Non-Fiction    959
		Poetry         464
		Drama          441
		Fiction        299
		Biography       67
		Sermon          48
		Letter          44
		Oratory         30
		Miscellany      26
		Dialogue         5
		Trial            2
		French           1
		Non-English      1
		```

* **Medium**:
	* Verse or Prose. Automatically determined (accuracy unknown):
		```python
		# The medium of a text is 'verse'
		# if the number of line tags outnumbers the number of paragraph tags:
		medium = 'Verse' if tag_counts['</L>'] > tag_counts['</P>'] else 'Prose'
		```
	* A separate 'mixed' designation was manually assigned to two texts (miscellanies).
	* Distribution:
		```
		Prose    1644
		Verse     741
		Mixed       2
		```

* **Form**:
	* A more specific genre designation, done manually by Ryan Heuser, so far applied only to what were originally declared non-fiction texts in "Genre" (see above). (Errors in the "Genre" tag were then manually corrected).
	* Distribution (>5 occurrences):
		```
		Essay                  297
		Fiction                143
		Pamphlet                97
		History                 79
		Treatise                71
		Letter                  53
		Sermon                  48
		Travel                  44
		Biography               28
		Satire                  23
		Oratory                 20
		Comedy                  13
		Memoir                  13
		Tract                    9
		Practical Treatise       9
		Periodical               8
		Criticism                7
		Panegyric                7
		Lecture                  7
		Tragedy                  6
		Dialogue                 6
		```

* **Topic**:
	* Another incomplete ad hoc designation by Ryan Heuser.

### XML Files

Were downloaded from [here](https://www.textcreationpartnership.org/docs/texts/ecco_files.html) (all the XML batch files).

### Plain text files

Were converted from XML using the function `text_plain_from_xml` in [lltk/corpus/tcp/tcp.py](https://github.com/quadrismegistus/lltk/blob/09e46c010a88a27df8186bd8e42a492bbf81c772/corpus/tcp/tcp.py#L40).

### Header and schema files

Were downloaded from [here](https://www.textcreationpartnership.org/docs/texts/ecco_files.html).
