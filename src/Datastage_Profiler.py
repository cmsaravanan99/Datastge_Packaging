import xmltodict
import json
import pandas as pd
import os

in_path=r'C:\Users\cmsar\Downloads\input'
out_path=r'C:\Users\cmsar\Downloads\output'
out_file = 'DataStage_Profile.csv'
stage_pins = []
def profiling(in_file,file):
    
    file_name=file.replace('.xml','')
    
    xml_str=open(in_file).read()
    
    ord_dict = xmltodict.parse(xml_str)
    
    xml_dict = json.loads(json.dumps(ord_dict))
    
    pin_partners = dict()
    pin_dslinks = dict()
    
    for Record in xml_dict['DSExport']['Job']['Record']:
        if Record['@Type'] in ['CustomOutput','CustomInput','TrxInput','TrxOutput']:
            for Property in Record['Property']:
                if Property['@Name']=='Partner':
                    pin_partners[Record['@Identifier']]= Property['#text']
                if Property['@Name']=='Name':
                    pin_dslinks[Record['@Identifier']]= Property['#text']
    
    
    
    for Record in xml_dict['DSExport']['Job']['Record']:
        if Record['@Type'] in ['CustomStage','TransformerStage']:
            pins_dict = dict()
            in_xml_dict=dict()
            #pins_dict['Identifier']=Record['@Identifier']
            pins_dict['File_Name']=file_name
            pins_dict['Stage_Name']=None
            pins_dict['Input_Links']=None
            pins_dict['Output_Links']=None
            #pins_dict['InputPins']=None
            #pins_dict['InputPins_Partner']=None
            #pins_dict['OutputPins']=None
            #pins_dict['OutputPins_Partner']=None
            for Property in Record['Property']:
                if Property['@Name']=='InputPins':
                    #pins_dict['InputPins']= Property['#text']
                    InputPins_Partner=''
                    InputPins_DSLinks=''
                    for pin in Property['#text'].split('|'):
                        #InputPins_Partner+=pin_partners[pin].replace('|','-')+'|'
                        InputPins_DSLinks+=pin_dslinks[pin]+'|'
                    InputPins_Partner=InputPins_Partner[:-1]
                    InputPins_DSLinks=InputPins_DSLinks[:-1]
                    #pins_dict['InputPins_Partner']=InputPins_Partner
                    pins_dict['Input_Links']=InputPins_DSLinks
        
                if Property['@Name']=='OutputPins':
                    #pins_dict['OutputPins']= Property['#text']
                    OutputPins_Partner=''
                    OutputPins_DSLinks=''
                    for pin in Property['#text'].split('|'):
                        #OutputPins_Partner+=pin_partners[pin].replace('|','-')+'|'
                        OutputPins_DSLinks+=pin_dslinks[pin]+'|'
                    OutputPins_Partner=OutputPins_Partner[:-1]
                    OutputPins_DSLinks=OutputPins_DSLinks[:-1]
                    #pins_dict['OutputPins_Partner']=OutputPins_Partner
                    pins_dict['Output_Links']=OutputPins_DSLinks
                if Property['@Name']=='Name':
                    pins_dict['Stage_Name']= Property['#text']
                if Property['@Name']=='StageType':
                    pins_dict['Stage_Type']= Property['#text']
            if pins_dict['Output_Links']==None:
                pins_dict['Type']='Target'
            elif pins_dict['Input_Links']==None:
                pins_dict['Type']='Source'
            else:
                pins_dict['Type']='Transformation'
            
            #Check each record to find out whether it contains another XML 
            for Property in Record['Property']:
                if isinstance(Record['Collection'],dict):
                    if Record['Collection']['@Name']=='Properties':
                        for SubRecord in Record['Collection']['SubRecord']:
                            count=0
                            for P in SubRecord['Property']:
                                if P['#text']=='XMLProperties':
                                    count+=1
                            if count==1:
                                if P['@Name']=='Value':
                                    #in_xml_dict['Stage_Name']=Record['Property'][0]['#text']
                                    in_xml_dict['SQL']=P['#text']
                                    
            #Get a dictnory in_xml_dict, which contains inner XML code and check does it SQL code or not
            def sql_check(in_xml_dict):
                if len(in_xml_dict)>0:
                    if in_xml_dict['SQL']:
                        i_ord_dict=xmltodict.parse(in_xml_dict['SQL'])
                        i_xml_dict=json.loads(json.dumps(i_ord_dict))
                            
                        if (i_xml_dict['Properties']['Usage']['SQL']):
                            for SQL in i_xml_dict['Properties']['Usage']['SQL']:
                                if SQL == 'SelectStatement':
                                    SelectStatement=i_xml_dict['Properties']['Usage']['SQL']['SelectStatement']
                                    return SelectStatement['#text']
                                
            #sql_check(in_xml_dict) returns SQL code and below it is going to be sent into a file and add as column in profile
            
            sql_file_path="C:\\Users\\cmsar\Documents\SQL_Override_files"
            def sql_write_up(sql,sql_file_path,n):
                naming="\\"+xml_dict['DSExport']['Job']['@Identifier']+'_'+n+".sql"
                path=sql_file_path+naming
                with open(path,'w') as f:
                    f.write(sql)
                return naming
            if isinstance(sql_check(in_xml_dict),str):
                sql_write_up(sql_check(in_xml_dict),sql_file_path,pins_dict['Stage_Name'])
                pins_dict['SQL Override File']=sql_write_up(sql_check(in_xml_dict),sql_file_path,pins_dict['Stage_Name'])[1:]
            stage_pins.append(pins_dict)
            
    stage_pins_df = pd.DataFrame(stage_pins)
    stage_pins_df.to_csv(out_file,index=False)

files = os.listdir(in_path)

for file in files:
    if '.xml' in file:
        in_file = in_path+'//'+file
        profiling (in_file,file)
