import subprocess
import re
import sys
import time


def remove_empty(l: list):
    while "" in l:
        l.remove("")
    return l


def check():
    res = subprocess.check_output("mlaunch list --dir /data/db", shell=True)
    res = res.decode('ascii')
    lines = res.split('\n')
    remove_empty(lines)

    output = []
    for i, line in enumerate(lines):
        if i == 0:
            heads = re.split(r'\s', line)
            remove_empty(heads)
        else:
            if i in [2, 3, 4]:
                items = re.split(r'\s', line)
                remove_empty(items)
                items[0] = ' '.join((items[0], items[1]))
                items.pop(1)
            elif i in [6, 7, 8]:
                items = re.split(r'\s', line)
                remove_empty(items)
                items[0] = '-'.join(("beijing", items[0]))
            elif i in [10, 11, 12]:
                items = re.split(r'\s', line)
                remove_empty(items)
                items[0] = '-'.join(("hongkong", items[0]))
            else:
                continue

            output.append(dict(zip(heads, items)))
    return output


def check_health():
    res = check()
    for p in res:
        if p['STATUS'] != "running":
            return False
    return True


def loop_watch():
    while True:
        if not check_health():
            subprocess.call("mlaunch start --dir /data/db", shell=True)
        time.sleep(2)


if __name__ == '__main__':
    if len(sys.argv) > 1:
        cmd = sys.argv[1]
        if cmd == "raw":
            res = check()
            for l in res:
                print(l)
        if cmd == "watch":
            loop_watch()
    else:
        exit(1)
