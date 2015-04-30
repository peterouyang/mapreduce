import mwparserfromhell
import xml.etree.ElementTree as ET
from mrjob.job import MRJob
from mrjob.step import MRStep
import heapq
import re

WORD_RE = re.compile(r"[a-zA-Z]+")

class MRWordCount(MRJob):

	limit = 300
	n = 100

	def mapper_init(self):
		self.text_chunk = ''
	
	def mapper(self, _, line):
		s = line.strip()
		if s == '<page>':
			self.text_chunk = s + '\n'
		elif s == '</page>':
			try:
				self.text_chunk += s +'\n'
				root = ET.fromstring(self.text_chunk)

				for x in list(root.iter()):
					if x.tag == 'text':
						extract = x.text
				par_text = mwparserfromhell.parse(extract)
				for template in par_text.filter_templates():
				    par_text.remove(template)
				for wikilink in par_text.filter_wikilinks():
				    par_text.remove(wikilink)
				for external_link in par_text.filter_external_links():
				    par_text.remove(external_link)
				for tag in par_text.filter_tags():
				    par_text.remove(tag)

				for word in WORD_RE.findall(par_text):
					yield (word.lower(), 1)
				self.text_chunk = ''
			except:
				pass
		else:
			self.text_chunk += line +'\n'

	
	def reducer_init1(self):
		# initialize heap
		self.h = [(0,'')]*self.limit
		heapq.heapify(self.h)


	def reducer1(self, word, counts):
		x = (sum(counts), word)
		if x > self.h[0]:
			heapq.heapreplace(self.h,x)

	def reducer_final1(self):
		for item in self.h:
			yield (item[1],item[0])

	def reducer_init2(self):
		# initialize heap
		self.h = [(0,'')]*self.n
		heapq.heapify(self.h)


	def reducer2(self, word, counts):
		x = (sum(counts), word)
		if x > self.h[0]:
			heapq.heapreplace(self.h,x)

	def reducer_final2(self):
		for item in self.h:
			yield (item[1],item[0])

	def steps(self):
		return [
			MRStep(mapper_init=self.mapper_init,
					mapper=self.mapper,
					reducer_init=self.reducer_init1,
					reducer=self.reducer1,
					reducer_final=self.reducer_final1),
			MRStep(reducer_init=self.reducer_init2,
					reducer=self.reducer2,
					reducer_final=self.reducer_final2)
		]

if __name__ == '__main__':
	MRWordCount.run()