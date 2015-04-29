from mrjob.job import MRJob

class MRMatrixMultiply(MRJob):
	def pivot_mapper(self, _, line):
		mat_name, i, j, x = line.split(',')

		if mat_name == 'A':
			yield (j, ('A', i, int(x)))		#int(x) is A_ij
		elif mat_name == 'B':
			yield (i, ('B', j, int(x)))		#int(x) is B_jk

	def expand_reducer(self, _, values):
		values = [v for v in values]
		Avals = [(i, A_ij) for (mat_name, i, A_ij) in values if mat_name == 'A']
		Bvals = [(k, B_jk) for (mat_name, k, B_jk) in values if mat_name == 'B']
		for i, A_ij in Avals:
			for k, B_jk in Bvals:
				yield ((i, k), A_ij * B_jk)

	def sum_reducer(self, ik, values):
		yield ik, sum(values)

	def steps(self):
		return [
			self.mr(mapper=self.pivot_mapper,
			reducer=self.expand_reducer),
			self.mr(reducer=self.sum_reducer)]


if __name__ == '__main__':
	MRMatrixMultiply.run()