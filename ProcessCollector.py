
import diamond.collector
import time
import psutil
import re

class ProcessCollector(diamond.collector.Collector):
    
    def __init__(self, config, handlers):
        super(ProcessCollector, self).__init__(config, handlers)

        self.separate_pids = self.config['separate_pids'] == 'True'

        pats = self.config['patterns']
        self.patterns = []

        for pat in pats:
            self.patterns.append(re.compile(pat))

        self.processes = {}
        self.lastfill = time.time()
        self.fill_proc_set()

    def get_default_config_help(self):
        config_help = super(ProcessCollector, self).get_default_config_help()
        config_help['patterns'] = 'Regex patterns to match for process names'
        config_help['separate_pids'] = 'Should processes be separated by pid\'s to prevent name conflicts'
        return config_help

    def get_default_config(self):
        config = super(ProcessCollector, self).get_default_config()
        config['patterns'] = ['^carbon']
        config['separate_pids'] = True
        return config
    
    def fill_proc_set(self):
        """
        Check the current proc list for, scan for processes that match the patterns, new processes
        get added under their pid's
        """
        starttime = time.time()

        # scan through active processes
        for proc in psutil.process_iter():
            pid = proc.pid
            # is the pid in the dictionary
            if pid in self.processes:
                # is the name the same (avoid problems with pid reuse)
                if proc.name == self.processes[pid].name:
                    # then do nothing as this process is still there and still matches
                    continue
                else:
                    # otherwise this pid has been reused and does not match the filters
                    del self.processes[pid]
                    
            name = proc.name
            for pattern in self.patterns:
                if re.match(pattern, name):
                    self.processes[pid] = proc
                    break

        # print elapsed time
        elapsed = time.time() - starttime
            
    
    def collect(self):

        # first calculate CPU
        for pid, proc in self.processes.items():
            # check if the process has died, if so remove it from the process list
            if not proc.is_running():
                del self.processes[pid]
            else:
                cpu = proc.get_cpu_percent(interval=0)
                mem = proc.get_memory_info()

                metric_dir = proc.name.replace(".","_")

                if self.separate_pids:
                    metric_dir = metric_dir + "." + str(pid)

                self.publish('.'.join([metric_dir, 'cpu']), cpu)
                self.publish('.'.join([metric_dir, 'ram']), mem.rss)

        # if 1 minutes has passed since last refill:
        # THEN refill the process list, this avoids CPU spikes before checking CPU
        if (time.time() - self.lastfill) > 60:
            self.fill_proc_set()

            # update last fill time
            self.lastfill = time.time()

            
            





