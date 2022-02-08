from.import main
import xmltodict
import os
in_path=main.in_path()
out_path=main.out_path()
def ord_dict():
  xml_str=open(in_file).read()
  ord_dict=xmltodict.parse(xml_str)
  return ord_dict
files=os.listdir(in_path)
for file in files:
  if '.xml' in files:
    in_file=in_path+'//'+file
