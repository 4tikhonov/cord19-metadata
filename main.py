from commons import CORDCommons
from processor import CORDProcessor

maindir = "/Users/vyacheslavtykhonov/projects/CoronaWhy/covid-19-infrastructure/data/original/CORD-19-research-challenge"
cord=CORDCommons(maindir)
#print(cord.map2file)

c = CORDCommons(maindir)
df = c.load_metadata()
df = df[0:10]
print(df.index)

r = CORDProcessor(df, maindir)
r.has_pmc_xml_parse()
for x in r.papers:
    print(x)
#r.papers
