from mrjob.job import MRJob
import random 

''' MRapproxmedian.py uses reservoir sampling to estimate the median of a stream of
integers.

The mapper yields a list of ints of length res_size.
These lists are aggregated by the reducer and reducer_final returns the median of 
the concatenated lists (aggregate_lis) from each mapper.

Should be run from the command line as (can use 'python' if you default to python2):
python2 MRapproxmedian.py numbers.txt

'''

def lis_median(numberlist):
	''' function to compute median of a list
	'''
	count = len(numberlist)
	numberlist.sort()
	if count % 2 == 0:
		return (numberlist[int(count/2)] + numberlist[int(count/2 - 1)])/2
	else:
		return numberlist[int((count-1)/2)]

class MRMedian(MRJob):
	res_size = 20
	reservoir = [0]*res_size

	num_elts_scanned = 0
	aggregate_lis = []

	def mapper(self, _, line):
		x = int(line.strip())
		if self.num_elts_scanned < self.res_size:
			self.reservoir[self.num_elts_scanned] = x
		else:
			j = random.randint(0, self.num_elts_scanned)
			if j < self.res_size:
				self.reservoir[j] = x
		self.num_elts_scanned += 1

	def mapper_final(self):
		if self.num_elts_scanned >= self.res_size:
			yield ("sample", self.reservoir)
		else:
			yield ("sample", self.reservoir[:num_elts_scanned])

	# note the reducer takes as argument the generator 'samples'
	# which is aggregated from the outputs of mapper_final
	def reducer(self, key, samples):
		for s in samples:
			self.aggregate_lis += s

	def reducer_final(self):
		med = lis_median(self.aggregate_lis)
		yield ("estimated median", med)


if __name__ == '__main__':
	MRMedian.run()




