import os
import time
from sys import argv
from subprocess import run, PIPE, check_output, Popen


mainpath = '/scratch/mburns13/data/ISING_RESULTS/MQLib'
partition = 'ising'
limsub = 25
max_myjobs = 300
check_timer = 3
pause_timer_batch = 0.5
pause_timer = 0.2

def check_headroom():
    ps = Popen(('squeue', '-u', 'mburns13', '-p', partition), stdout=PIPE)
    output = check_output(('wc', '-l'), stdin=ps.stdout)
    ps.wait()
    return int(output)


def main(simpath: str):
    jobdir = os.path.join(mainpath, simpath, 'jobs')
    numjobs = len(os.listdir(jobdir))
    headroom = max_myjobs
    launched = 0
    while (launched < numjobs):
        headroom_ceil = check_headroom()
        headroom = min(max_myjobs - headroom_ceil, numjobs-launched)
        while (headroom > 0):
            for i in range(0, min(headroom, limsub)):
                launched += 1
                jfile = f"{jobdir}/mqlib_job_{launched}.sh"
                run(['sbatch', os.path.join(jobdir, jfile)], stdout=PIPE)
                headroom -= 1
                time.sleep(pause_timer)
            time.sleep(pause_timer_batch)
        time.sleep(check_timer)


if __name__ == '__main__':
    check_headroom()
    main(argv[1])
