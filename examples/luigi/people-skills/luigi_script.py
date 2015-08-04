import luigi
from subprocess import call

class InputTask0(luigi.Task):
    def output(self): return [luigi.LocalTarget('skills')]
    
class OutputTask0(luigi.Task):
    def requires(self): return [InputTask0()]
    def run(self): call('  grep -v BAD skills > skills.filtered;', shell=True)
    def output(self): return [luigi.LocalTarget('skills.filtered')]

class InputTask1(luigi.Task):
    def output(self): return [luigi.LocalTarget('people')]
    
class OutputTask1(luigi.Task):
    def requires(self): return [InputTask1(), OutputTask0()]
    def run(self): call('  awk -F, \'{ print $2 " " $1 ", " $3}\' people > people.fullname;', shell=True)
    def output(self): return [luigi.LocalTarget('people.fullname')]
    
class InputTask2(luigi.Task):
    def output(self): return [luigi.LocalTarget('people.fullname')]
    
class OutputTask2(luigi.Task):
    def requires(self): return [InputTask2(), OutputTask1()]
    def run(self): call('  sort people.fullname > people.sorted;', shell=True)
    def output(self): return [luigi.LocalTarget('people.sorted')]
    
class InputTask3(luigi.Task):
    def output(self): return [luigi.LocalTarget('skills.filtered'), luigi.LocalTarget('people.sorted')]
    
class OutputTask3(luigi.Task):
    def requires(self): return [InputTask3(), OutputTask2()]
    def run(self): call('  echo Number of inputs: 2;  echo All inputs: skills.filtered people.sorted;  echo Joining ...;  join -t, skills.filtered people.sorted > output', shell=True)
    def output(self): return [luigi.LocalTarget('output')]

if __name__ == '__main__':
    luigi.run()