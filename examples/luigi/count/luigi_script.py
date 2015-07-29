import luigi
from subprocess import call

class InputTask0(luigi.Task):
	def output(self): return luigi.LocalTarget('out.csv')
	
class OutputTask0(luigi.Task):
    def output(self): return luigi.LocalTarget('count.txt')
    def run(self): call('wc out.csv > count.txt', shell=True)
    
if __name__ == '__main__':
    luigi.run()