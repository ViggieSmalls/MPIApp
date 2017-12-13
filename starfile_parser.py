fname = '/tmp/test_mpiapp_input/20S_265_Mar29_01.38.49/micrographs_all_gctf.star'
myfile = open(fname, 'r')
content = myfile.read()
l = list(filter(None, content.split('\n')))
keys = []

for n, i in enumerate(l):
    if i.endswith('_'):
        keys.append(1 + keys[-1]) if bool(keys) else keys.append(1)
    elif i.startswith('_'):
        keys.append(0)
    else:
        keys.append(-1)
    print(keys[n], i)

