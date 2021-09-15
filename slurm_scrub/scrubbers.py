import os
import re
'''
Functions for querying sacct and slurm output files
'''

job_regex = re.compile(
    r'^.*?[_-](?P<jobid>(?P<job>[0-9]+)(_[0-9]+)?)(.out)?$')

#: Regex for DDHHMMSS style timestamps
DDHHMMSS_RE = re.compile(
    r'(?P<days>\d+)-(?P<hours>\d{2}):(?P<minutes>\d{2}):(?P<seconds>\d{2})')
#: Regex for HHMMSS style timestamps
HHMMSS_RE = re.compile(
    r'(?P<hours>\d{2}):(?P<minutes>\d{2}):(?P<seconds>\d{2})')
#: Regex for HHMMmmm style timestamps
MMSSMMM_RE = re.compile(
    r'(?P<minutes>\d{2}):(?P<seconds>\d{2}).(?P<milliseconds>\d{3})')

multiple_map = {
        'K': 1024 ** 0,
        'M': 1024 ** 1,
        'G': 1024 ** 2,
        'T': 1024 ** 3,
        'E': 1024 ** 4,
}


def get_slurm_out_stats(infile: str, verbose=False):
    match = job_regex.match(infile)
    if not match:
        if verbose:
            print(f'Cannot match regex for {infile}')
        return None

    jobid = match.group('jobid')
    num_inputs = 0
    input_size = 0
    with open(infile, 'r') as reader:
        for line in reader:
            line = line.strip()
            if line.startswith('input:'):
                if verbose:
                    print(f'Found input line -> {line}')
                tokens = line.split()[1:]
                num_inputs += len(tokens)
                input_size += sum(
                    [os.path.getsize(f.strip(',')) for f in tokens])

    return jobid, num_inputs, input_size


def get_sacct_info(jobs, verbose=False):
    cmd = 'sacct -P -n'.split()
    columns = [
        'JobIDRaw',
        'JobID',
        'State',
        'AllocCPUS',
        'TotalCPU',
        'Elapsed',
        'Timelimit',
        'REQMEM',
        'MaxRSS',
        'NNodes',
        'NTasks'
    ]
    cmd += [
        '--format=' + ','.join(columns),
        '--job=' + ','.join(jobs)
    ]

    sacct = subprocess.run(
        args=cmd,
        stdout=subprocess.PIPE,
        encoding='utf8',
        check=True,
        universal_newlines=True)

    if sacct.returncode != 0:
        raise Exception('Error running sacct!')

    sacct = [dict(zip(columns, line.split('|')))
              for line in sacct.stdout.split('\n') if line]

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
