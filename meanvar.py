from mrjob.job import MRJob
''' meanvar.py uses Map-Reduce to compute the mean and variance of a series of integers

	file format should be integers with each on a distinct line.

	run from command line as:

	python2 meanvar.py <file of numbers>
'''



class MRMeanVar(MRJob):
	sq = 0
	acc = 0
	num = 0
	total_sq = 0
	total_acc = 0
	total_num = 0


	def mapper(self, _,line):
		self.acc += int(line.strip())
		self.sq += int(line.strip())*int(line.strip())
		self.num += 1

	def mapper_final(self):
		yield ("sum", self.acc)
		yield ("count entries", self.num)
		yield ("sum sq", self.sq)

	def reducer(self, value_type, x):
		if value_type == "sum":
			self.total_acc = sum(x)
		elif value_type == "count entries":
			self.total_num = sum(x)
		elif value_type == "sum sq":
			self.total_sq = sum(x)

	def reducer_final(self):
		mean = float(self.total_acc)/float(self.total_num)
		print("Mean: ", mean)
		var = float(self.total_sq)/float(self.total_num) \
				- mean*mean
		print("Variance: ", var)

if __name__ == '__main__':
	MRMeanVar.run()