import re
import os
from typing import Dict
from datetime import timedelta


multiple_map = {
        'K': 1024 ** 0,
        'M': 1024 ** 1,
        'G': 1024 ** 2,
        'T': 1024 ** 3,
        'E': 1024 ** 4,
}

state_colors = {
    'FAILED': 'red',
    'TIMEOUT': 'red',
    'OUT_OF_MEMORY': 'red',
    'RUNNING': 'cyan',
    'CANCELLED': 'yellow',
    'COMPLETED': 'green',
    'PENDING': 'blue',
}

#: Regex for DDHHMMSS style timestamps
DDHHMMSS_RE = re.compile(
    r'(?P<days>\d+)-(?P<hours>\d{2}):(?P<minutes>\d{2}):(?P<seconds>\d{2})')
#: Regex for HHMMSS style timestamps
HHMMSS_RE = re.compile(
    r'(?P<hours>\d{2}):(?P<minutes>\d{2}):(?P<seconds>\d{2})')
#: Regex for HHMMmmm style timestamps
MMSSMMM_RE = re.compile(
    r'(?P<minutes>\d{2}):(?P<seconds>\d{2}).(?P<milliseconds>\d{3})')


class Job():
    def __init__(self, job: str, jobid: str, filename: str):
        self.job = job
        self.jobid = jobid
        self.filename = filename
        self.stepmem = 0
        self.totalmem = None
        self.time = '---'
        self.time_eff = '---'
        self.elapsed_seconds = None
        self.cpu = '---'
        self.mem = '---'
        self.state = None
        self.num_inputs = 0
        self.input_size = 0
        self.other_entries = {}

    def __eq__(self, other):
        if not isinstance(other, Job):
            return False

        return self.__dict__ == other.__dict__

    def __repr__(self):
        return (f'Job(job={self.job}, jobid={self.jobid}, '
                f'filename={self.filename})')

    def input_stats(self, directory: str):
        '''
        Sets total number of input files and their total size
        assuming typical snakemake log information
        Sets elapsed seconds for scrubbing
        Group jobs are detected by having more than one job count in header
        For group jobs, all rules matching the first rule are counted as input
        This should handle most cases of grouping as either
        - the first step in a pipeline
        - grouping of smaller jobs
        If size is present, use that instead of os.path.getsize
        '''
        if self.filename is None or self.state != 'COMPLETED':
            return
        fname = self.filename
        if directory:
            fname = os.path.join(directory, fname)
        first_job = None

        with open(fname, 'r') as reader:
            for line in reader:
                line = line.strip()

                # piping usually
                if line.startswith('group job ') and first_job is None:
                    # processes first group and gets input size and inputs
                    first_job = self.find_first_job(reader)

                # 'normal' groups, just get first job
                if first_job is None and (
                    line.startswith('rule') or
                    line.startswith('checkpoint')
                ):
                    first_job = line

                # process any other groups
                if line == first_job:
                    inputs, sizes = self.process_rule(reader)
                    self.num_inputs += inputs
                    self.input_size += sizes

        self.elapsed_seconds = _parse_slurm_timedelta(self.time)

    def process_rule(self, reader):
        '''
        given the slurm output from snakemake, find the number of inputs
        and file sizes from either the filenames or the sizes (if available)
        '''
        for line in reader:
            line = line.strip()
            if line.startswith('input:'):
                input_line = line

            if line.startswith('size:'):
                tokens = line.split()[1:]
                break

            if line.startswith('output:'):  # haven't found size
                tokens = [os.path.getsize(i)
                          for i in input_line.split()[1:]
                          if os.path.exists(i)]
                break

        return len(tokens), self.get_sizes(tokens)

    def get_sizes(self, tokens):
        result = 0
        for t in tokens:
            try:
                result += int(t.strip(','))
            except ValueError:
                pass
        return result

    def find_first_job(self, reader):
        '''
        Assuming reader is starting at a group job slurm log,
        extract rule inputs, outputs and sizes to determine which is
        first, i.e. the rule with inputs that don't depend on other outputs
        Updates the number of inputs and sizes and returns the job name
        '''
        jobs = []
        for line in reader:
            # no longer indented, next group
            if not line.startswith(' ') and line != '\n':
                break

            line = line.strip()

            if line.startswith('rule'):
                jobs.append({'name': line})
            elif line.startswith('input'):
                jobs[-1]['inputs'] = [l.strip(',') for l in line.split()[1:]]
            elif line.startswith('size'):
                jobs[-1]['sizes'] = [l.strip(',') for l in line.split()[1:]]
            elif line.startswith('output'):
                jobs[-1]['outputs'] = [l.strip(',') for l in line.split()[1:]]

        def in_outputs(infiles):
            for infile in infiles:
                for job in jobs:
                    for outfile in job['outputs']:
                        if infile == outfile:
                            return True
            return False

        for job in jobs:
            # see if an infile is another jobs outfile
            if not in_outputs(job['inputs']):
                # use sizes if they exist, else get file sizes
                tokens = job['sizes'] if 'sizes' in job else \
                    [os.path.getsize(i)
                     for i in job['inputs']
                     if os.path.exists(i)]

                self.num_inputs += len(tokens)
                self.input_size += self.get_sizes(tokens)

                return job['name']

    def get_scrub_entry(self):
        return {
            'name': self.other_entries['JobName'],
            'id': self.other_entries['JobID'],
            'inputs': self.num_inputs,
            'size': self.input_size,
            'time': self.elapsed_seconds,
            'mem': self.stepmem,
            'cores': self.other_entries['AllocCPUS'],
            'nodes': self.other_entries['NNodes'],
        }

    def update(self, entry: Dict):
        if '.' not in entry['JobID']:
            self.state = entry['State'].split()[0]

        if self.state == 'PENDING':
            return

        # master job id
        if self.jobid == entry['JobID']:
            self.other_entries = entry
            self.time = entry['Elapsed'] if 'Elapsed' in entry else None
            requested = _parse_slurm_timedelta(entry['Timelimit']) \
                if 'Timelimit' in entry else 1
            wall = _parse_slurm_timedelta(entry['Elapsed']) \
                if 'Elapsed' in entry else 0
            self.time_eff = round(wall / requested * 100, 1)
            if self.state == 'RUNNING':
                return
            cpus = (_parse_slurm_timedelta(entry['TotalCPU']) /
                    int(entry['AllocCPUS'])) \
                if 'TotalCPU' in entry and 'AllocCPUS' in entry else 0
            if wall == 0:
                self.cpu = None
            else:
                self.cpu = round(cpus / wall * 100, 1)
            self.totalmem = parsemem(entry['REQMEM'],
                                     int(entry['NNodes']),
                                     int(entry['AllocCPUS'])) \
                if 'REQMEM' in entry and 'NNodes' in entry \
                and 'AllocCPUS' in entry else None

        elif self.state != 'RUNNING':
            for k, v in entry.items():
                if k not in self.other_entries or not self.other_entries[k]:
                    self.other_entries[k] = v
            self.stepmem += parsememstep(entry['MaxRSS']) \
                if 'MaxRSS' in entry else 0

    def name(self):
        if self.filename:
            return self.filename
        else:
            return self.jobid

    def get_entry(self, key):
        if key == 'JobID':
            return self.name()
        if key == 'State':
            return self.state
        if key == 'MemEff':
            if self.totalmem:
                value = round(self.stepmem/self.totalmem*100, 1)
            else:
                value = '---'
            return value
        if key == 'TimeEff':
            return self.time_eff
        if key == 'CPUEff':
            return self.cpu if self.cpu else '---'
        else:
            return self.other_entries.get(key, '---')


def _parse_slurm_timedelta(delta: str) -> int:
    """Parse one of the three formats used in TotalCPU
    into a timedelta and return seconds."""
    match = re.match(DDHHMMSS_RE, delta)
    if match:
        return int(timedelta(
            days=int(match.group('days')),
            hours=int(match.group('hours')),
            minutes=int(match.group('minutes')),
            seconds=int(match.group('seconds'))
        ).total_seconds())
    match = re.match(HHMMSS_RE, delta)
    if match:
        return int(timedelta(
            hours=int(match.group('hours')),
            minutes=int(match.group('minutes')),
            seconds=int(match.group('seconds'))
        ).total_seconds())
    match = re.match(MMSSMMM_RE, delta)
    if match:
        return int(timedelta(
            minutes=int(match.group('minutes')),
            seconds=int(match.group('seconds')),
            milliseconds=int(match.group('milliseconds'))
        ).total_seconds())


def parsemem(mem: str, nodes: int, cpus: int):
    multiple = mem[-2]
    alloc = mem[-1]

    mem = float(mem[:-2]) * multiple_map[multiple]

    if alloc == 'n':
        return mem * nodes
    else:
        return mem * cpus


def parsememstep(mem: str):
    try:
        if mem == '':
            return 0
        multiple = mem[-1]

        mem = float(mem[:-1]) * multiple_map[multiple]

        return mem

    except ValueError:
        raise ValueError(f'Unexpected memstep format: {mem}')
