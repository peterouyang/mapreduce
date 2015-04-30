from mrjob.job import MRJob
from mrjob.step import MRStep
import heapq
import re

WORD_RE = re.compile(r"[\w]+")


class MRWordCount(MRJob):

	limit = 200
	n = 100

	
	def mapper(self, _, line):
		# yield (word, 1) for each 
		for word in WORD_RE.findall(line):
			yield (word.lower(), 1)

	
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
			MRStep(mapper=self.mapper,
					reducer_init=self.reducer_init1,
					reducer=self.reducer1,
					reducer_final=self.reducer_final1),
			MRStep(reducer_init=self.reducer_init2,
					reducer=self.reducer2,
					reducer_final=self.reducer_final2)
		]

if __name__ == '__main__':
	MRWordCount.run()