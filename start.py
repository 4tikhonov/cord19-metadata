from commons import CORDCommons
from processor import CORDProcessor
from api.config import maindir

cord=CORDCommons(maindir)
c = CORDCommons(maindir)
df = c.load_metadata()
fulldf = df
df = df[0:10]
print(df.index)

r = CORDProcessor(df, maindir)
p = CORDProcessor(fulldf, maindir)
p.has_no_pmc_xml_parse()
print(p.papers)
