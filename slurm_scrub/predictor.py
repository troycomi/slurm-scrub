import numpy as np
import os
import json
import click


class Predictor():
    def __init__(self, json_file):
        self.json = json_file
        self.load_fits()

    def load_fits(self):
        self.modified_time = os.path.getmtime(self.json)
        with open(self.json, 'r') as jsn:
            self.fits = json.load(jsn)
        for k, f in self.fits.items():
            for k1, f1 in f.items():
                self.fits[k][k1]['params'] = \
                    np.array(self.fits[k][k1]['params'])

    def estimate(self, rulename, filesize, resource, default, attempt=1):
        filesize /= 2**20  # convert bytes to MB
        if self.need_update():
            click.secho('Updating fit cache', fg='yellow')
            self.load_fits()

        if rulename not in self.fits:
            click.secho(f'"{rulename}" not found in fit database',
                        fg='yellow')
            return default * attempt

        fit = self.fits[rulename]
        if fit['time']['samples'] < 10:
            click.secho(f'"{rulename}" only has {fit["time"]["samples"]} '
                        'samples, using default values',
                        fg='yellow', bold=True)
            return default * attempt

        if (filesize < fit['time']['range'][0] / 2 or
                filesize > fit['time']['range'][1] * 1.5):
            click.secho(f'"{rulename}" has input size out of known range, '
                        'using default values',
                        fg='yellow', bold=True)
            return default * attempt

        # want to replace entries in defaults with fits
        # short_jobs is special and is added/removed based on time
        # if key in default not in fit, warn and use default
        if resource == 'short_jobs':
            k = 'time'
        else:
            k = resource

        if k not in fit:
            click.secho(f'"{rulename}" has no estimate for {k}, '
                        'using default', fg='yellow', bold=True)
            result = default

        else:
            if fit[k]['mean_eff'] < 50:
                click.secho(f'"{rulename}" has low estimated efficiency, '
                            f'consider a different model fit for {k}!',
                            fg='yellow', bold=True)
            if fit[k]['failures']/fit[k]['samples'] > 0.05:
                click.secho(f'"{rulename}" more than 5% training failures,'
                            f' consider a different model fit for {k}!',
                            fg='yellow', bold=True)
            result = int(round(np.polyval(fit[k]["params"], filesize)))

        # increase resource based on attempts
        result *= attempt

        # round up 45 minutes to 62 to use fewer short jobs
        if k == 'time':
            result = 62 if result > 45 and result < 62 else result

        if resource == 'short_jobs':
            return 1 if result <= 61 else 0

        return result

    def need_update(self):
        return self.modified_time != os.path.getmtime(self.json)
