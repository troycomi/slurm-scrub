import click
import subprocess
import pandas as pd
import numpy as np
from io import StringIO
import os
from slurm_scrub.estimator import Estimator
import json
from threadpoolctl import threadpool_limits


@click.group()
def main():
    pass


@main.command()
@click.option('--db', default='~/slurm-scrub.tsv.gz',
              help='gzipped tsv database location')
@click.argument('jobs', nargs=-1)
def record(db, jobs):
    db = os.path.expanduser(db)
    result = subprocess.run(
        ('sscrub jobs ' + ' '.join(jobs)).split(),
        stdout=subprocess.PIPE,
        encoding='utf8',
        check=True,
        universal_newlines=True)

    if result.returncode != 0:
        raise Exception('Error running sscrub!')

    job_data = pd.read_csv(StringIO(result.stdout),
                           sep='\t', index_col=['name', 'id'])
    old_size = 0
    if os.path.exists(db):
        old_data = pd.read_csv(db, sep='\t', index_col=['name', 'id'])
        old_data = old_data.loc[~old_data.index.duplicated(keep='first')]
        old_size = len(old_data)
        job_data = pd.concat([old_data[~old_data.index.isin(job_data.index)],
                              job_data])
    job_data.to_csv(db, compression='gzip', sep='\t')
    new_size = len(job_data)
    click.echo(f'Added {new_size - old_size} new entries')


@main.command()
@click.option('--db', default='~/slurm-scrub.tsv.gz',
              help='gzipped tsv database location')
@click.option('--rule', default=None,
              help='rule name to predict')
@click.option('--size', default=None, type=float,
              help='input file size, in MB')
@click.option('--output', type=click.File('w'),
              default=None,
              help='where to save fit information as json')
def predict(db, rule, size, output):
    job_data = pd.read_csv(db, sep='\t', index_col=['name', 'id'])
    job_data = job_data.loc[~job_data.index.duplicated(keep='first')]
    if rule is not None:
        job_data = job_data.xs(rule, level='name', drop_level=False)

    estimator = Estimator()
    fits = {}
    with threadpool_limits(limits=4):
        for group_name, df in job_data.groupby('name'):
            fits[group_name] = {
                # convert size from byte to MB and time from seconds to minutes
                'time': estimator.fit(df['size']/2**20, df['time']/60,
                                      min_deg=2),
                # convert mem from KB to MB
                'mem': estimator.fit(df['size']/2**20, df['mem']/2**10),
            }

    if size is not None:
        for k, f in fits.items():
            mem = np.polyval(f['mem']['params'], size)
            time = np.polyval(f['time']['params'], size)
            note = (' *' if size < f['mem']['range'][0] or
                    size > f['mem']['range'][1] else '')
            click.echo(f'{k:>20}{mem:>10.0f} MB {time:>10.0f} min{note}')
        return

    if output is not None:
        for k, f in fits.items():
            for k1, f1 in f.items():
                fits[k][k1]['params'] = fits[k][k1]['params'].tolist()
        json.dump(fits, output)

    else:
        for k, f in fits.items():
            click.echo(f'{k} ->')
            for k1, f1 in f.items():
                click.echo(f'\t{k1}->')
                for k2, f2 in f1.items():
                    click.echo(f'\t\t{k2}->{f2}')


if __name__ == '__main__':
    main()
