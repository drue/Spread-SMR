import sys, os

def gname(f):
    return f + ".gl"

for f in sys.argv[1:]:
    os.system('grep "Marking Green" %s > %s' % (f, gname(f)))

l = [gname(x) for x in sys.argv[1:]]
for f in l[1:]:
    os.system('diff -u %s %s' % (l[0], f))

    
    
