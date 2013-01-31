# coding=utf-8

"""
A Diamond collector that collects memory usage of each process defined in it's
config file by matching them with their executable filepath or the process name.

Example config file ProcessCollector.conf

```
enabled=True
unit=kB
separate_pids=True
[process]
[[postgres]]
exe=^\/usr\/lib\/postgresql\/+d.+d\/bin\/postgres$
name=^postgres,^pg
```

exe and name are both lists of comma-separated regexps.
"""

import re
import diamond.collector
import diamond.convertor
import time

try:
    import psutil
    psutil
except ImportError:
    psutil = None


def process_filter(proc, cfg):
    """
    Decides whether a process matches with a given process descriptor

    :param proc: a psutil.Process instance
    :param cfg: the dictionary from processes that describes with the
        process group we're testing for
    :return: True if it matches
    :rtype: bool
    """
    for exe in cfg['exe']:
        try:
            if exe.search(proc.exe):
                return True
        except psutil.AccessDenied:
            break
    for name in cfg['name']:
        if name.search(proc.name):
            return True
    for cmdline in cfg['cmdline']:
        if cmdline.search(' '.join(proc.cmdline)):
            return True
    return False


class ProcessCollector(diamond.collector.Collector):

    def __init__(self, config, handlers):
        super(ProcessCollector, self).__init__(config, handlers)
        self.last_reload = 0

    def get_default_config_help(self):
        config_help = super(ProcessCollector, self).get_default_config_help()
        config_help.update({
            'unit': 'The unit in which memory data is collected.',
            'process': ("A subcategory of settings inside of which each "
                        "collected process has it's configuration"),
            'separate_pids': 'Append process names with their PID\'s to prevent name conflicts.',
            'naming_method': 'Specifies under what name to post metrics. Options: process_name, config_title'
        })
        return config_help

    def get_default_config(self):
        """
        Default settings are:
            path: 'process_stats'
            unit: 'B'
            separate_pids: True
            naming_method: process_name
        """
        config = super(ProcessCollector, self).get_default_config()
        config.update({
            'path': 'process_stats',
            'unit': 'B',
            'process': '',
            'separate_pids': True,
            'naming_method': 'process_name'
            })
        return config

    def setup_config(self):
        """
        prepare self.processes, which is a descriptor dictionary in
        processgroup --> {
            exe: [regex],
            name: [regex],
            cmdline: [regex],
            procs: {pid => psutil.Process}
            naming_method: [string]
        }
        """
        self.processes = {}
        for process, cfg in self.config['process'].items():
            # first we build a dictionary with the process aliases and the
            #  matching regexps
            proc = {'procs': {}}
            for key in ('exe', 'name', 'cmdline'):
                proc[key] = cfg.get(key, [])
                if not isinstance(proc[key], list):
                    proc[key] = [proc[key]]
                proc[key] = [re.compile(e) for e in proc[key]]
            if cfg.has_key('naming_method'):
                proc['naming_method'] = cfg.get('naming_method')
            self.processes[process] = proc

    def filter_processes(self):
        """
        Populates self.processes[processname]['procs'] with the corresponding
        list of psutil.Process instances
        """

        for proc in psutil.process_iter():
            # filter and divide the system processes amongst the different
            #  process groups defined in the config file
            for procname, cfg in self.processes.items():
                if process_filter(proc, cfg):
                    cfg['procs'][proc.pid] = proc
                    break

    def collect(self):
        """
        Collects the CPU and memory usage of each process defined under the
        `process` subsection of the config file
        """

        # Only reload the process list every 10 seconds,
        if time.time() - self.last_reload > 10:
            self.setup_config()
            self.filter_processes()
            self.last_reload = time.time()

        unit = self.config['unit']
        naming_method = self.config.get('naming_method', 'process_name')
        for process, cfg in self.processes.items():
            for pid, proc in cfg['procs'].items():
                if proc.is_running():
                    cpu = proc.get_cpu_percent(interval=0)
                    mem = proc.get_memory_info().rss

                    metric_prefix = process if cfg.get('naming_method', naming_method) == 'config_title' else proc.name
                    metric_prefix = metric_prefix.replace('.', '_')

                    if self.config['separate_pids']:
                        metric_prefix = '.'.join([metric_prefix, str(pid)])

                    metric_name = '.'.join([metric_prefix, 'cpu'])
                    metric_value = cpu
                    self.publish(metric_name, metric_value)

                    metric_name = '.'.join([metric_prefix, 'ram'])
                    metric_value = diamond.convertor.binary.convert(mem, oldUnit='byte', newUnit=unit)
                    self.publish(metric_name, metric_value)