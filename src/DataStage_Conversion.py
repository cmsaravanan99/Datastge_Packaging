# -*- coding: utf-8 -*-
"""
Data Stage Migration Utility
Author: Systech Solutions, Inc.
"""

import xmltodict
import json
import os

in_path = r'C:\Users\NNalband\Documents\Project\input'
out_path = r'C:\Users\NNalband\Documents\Project\output'

def find_dict_keys(json_dict):
    dict_keys=[]
    def _find_dict_keys(json_dict):
        if isinstance(json_dict, list):
            for item in json_dict:
                _find_dict_keys(item)
        elif isinstance(json_dict, dict):
            for item in json_dict:
                if isinstance(json_dict[item], dict):
                    dict_keys.append(item)
                    _find_dict_keys(json_dict[item])
    _find_dict_keys(json_dict)
    return dict_keys

def xml_to_dict(xml_str):
    xml_str=open(in_file).read()
    ord_dict = xmltodict.parse(xml_str)
    temp_xml_dict = json.loads(json.dumps(ord_dict))
    #print(type(temp_xml_dict)==dict)
    dict_keys = find_dict_keys(temp_xml_dict)
    print(dict_keys)
    ord_dict = xmltodict.parse(xml_str, force_list=dict_keys)
    xml_dict = json.loads(json.dumps(ord_dict))
    #print(xml_dict['DSExport'])
    return xml_dict

def xml_conversion (in_file, out_file):

    xml_str=open(in_file).read()
    
    #ord_dict = xmltodict.parse(xml_str)
    #xml_dict = json.loads(json.dumps(ord_dict))
    xml_dict=xml_to_dict(xml_str)
    
    reconstructed_json=json.dumps(xml_dict)
    writer = open(r'C:\Users\NNalband\Documents\Project\Datastage_reconstructed_JSON.json','w')
    writer.write(reconstructed_json)
    writer.close()
    
    stages = 0
    tgt_record_identifier=''
    for DSExport in xml_dict['DSExport']:
        #print(Job)
        for Job in DSExport['Job']:
            for Record in Job['Record']:
                if Record['@Type']=='ContainerView':
                    for Property in Record['Property']:
                        if Property['@Name']=='StageTypeIDs':
                            stages=len(Property['#text'].split('|'))
                            Property['#text']='|'.join(Property['#text'].split('|')[:-1])+'|NetezzaConnectorPX'
                    for Property in Record['Property']:
                        if Property['@Name']=='StageList':
                            tgt_record_identifier=Property['#text'].split('|')[stages-1]    
            
            SubRecord_Property_Changes={'VariantName':'4.5',
                                        'VariantLibrary':'ccnz',
                                        'SupportedVariants':'V1;4.5::ccnz',
                                        'SupportedVariantsLibraries':'ccnz',
                                        'ConnectorName':'NetezzaConnector',
                                        'XMLProperties':'&lt;?xml version=&apos;1.0&apos; encoding=&apos;UTF-16&apos;?&gt;&lt;Properties version=&apos;1.1&apos;&gt;&lt;Common&gt;&lt;Context type=&apos;int&apos;&gt;2&lt;/Context&gt;&lt;Variant type=&apos;string&apos;&gt;4.5&lt;/Variant&gt;&lt;DescriptorVersion type=&apos;string&apos;&gt;1.0&lt;/DescriptorVersion&gt;&lt;PartitionType type=&apos;int&apos;&gt;-1&lt;/PartitionType&gt;&lt;RCP type=&apos;int&apos;&gt;1&lt;/RCP&gt;&lt;/Common&gt;&lt;Connection&gt;&lt;DataSource modified=&apos;1&apos; type=&apos;string&apos;&gt;&lt;![CDATA[Demo_Training]]&gt;&lt;/DataSource&gt;&lt;Database modified=&apos;1&apos; type=&apos;string&apos;&gt;&lt;![CDATA[nz_datastage]]&gt;&lt;/Database&gt;&lt;Username modified=&apos;1&apos; type=&apos;string&apos;&gt;&lt;![CDATA[admin]]&gt;&lt;/Username&gt;&lt;Password modified=&apos;1&apos; type=&apos;protectedstring&apos;&gt;&lt;![CDATA[{iisenc}9ipdrPy+WSNO0MqykV0D7NSIFaSI9o1ikzRO4SJgjBU=]]&gt;&lt;/Password&gt;&lt;UseSeparateConnectionForTWT collapsed=&apos;1&apos; type=&apos;bool&apos;&gt;&lt;![CDATA[0]]&gt;&lt;/UseSeparateConnectionForTWT&gt;&lt;/Connection&gt;&lt;Usage&gt;&lt;WriteMode type=&apos;int&apos;&gt;&lt;![CDATA[0]]&gt;&lt;/WriteMode&gt;&lt;TableName modified=&apos;1&apos; type=&apos;string&apos;&gt;&lt;![CDATA[dbo.rbala_order_fact_ds]]&gt;&lt;/TableName&gt;&lt;EnableCaseSensitiveIDs type=&apos;bool&apos;&gt;&lt;![CDATA[0]]&gt;&lt;/EnableCaseSensitiveIDs&gt;&lt;TruncateColumnNames collapsed=&apos;1&apos; type=&apos;bool&apos;&gt;&lt;![CDATA[0]]&gt;&lt;/TruncateColumnNames&gt;&lt;SQL&gt;&lt;DirectInsert type=&apos;bool&apos;&gt;&lt;![CDATA[0]]&gt;&lt;/DirectInsert&gt;&lt;EnableRecordOrdering collapsed=&apos;1&apos; type=&apos;bool&apos;&gt;&lt;![CDATA[0]]&gt;&lt;/EnableRecordOrdering&gt;&lt;CheckDuplicateRows collapsed=&apos;1&apos; type=&apos;bool&apos;&gt;&lt;![CDATA[0]]&gt;&lt;/CheckDuplicateRows&gt;&lt;/SQL&gt;&lt;TableAction collapsed=&apos;1&apos; type=&apos;int&apos;&gt;&lt;![CDATA[0]]&gt;&lt;/TableAction&gt;&lt;Session&gt;&lt;SchemaReconciliation&gt;&lt;UnmatchedLinkColumnAction type=&apos;int&apos;&gt;&lt;![CDATA[1]]&gt;&lt;/UnmatchedLinkColumnAction&gt;&lt;TypeMismatchAction type=&apos;int&apos;&gt;&lt;![CDATA[1]]&gt;&lt;/TypeMismatchAction&gt;&lt;UnmatchedTableColumnAction type=&apos;int&apos;&gt;&lt;![CDATA[1]]&gt;&lt;/UnmatchedTableColumnAction&gt;&lt;MismatchReportingAction type=&apos;int&apos;&gt;&lt;![CDATA[2]]&gt;&lt;/MismatchReportingAction&gt;&lt;/SchemaReconciliation&gt;&lt;TemporaryWorkTable type=&apos;int&apos;&gt;&lt;![CDATA[0]]&gt;&lt;DropTable type=&apos;bool&apos;&gt;&lt;![CDATA[1]]&gt;&lt;/DropTable&gt;&lt;EnableMergeJoin type=&apos;int&apos;&gt;&lt;![CDATA[0]]&gt;&lt;/EnableMergeJoin&gt;&lt;/TemporaryWorkTable&gt;&lt;LoadOptions&gt;&lt;ValidatePKs collapsed=&apos;1&apos; type=&apos;bool&apos;&gt;&lt;![CDATA[0]]&gt;&lt;/ValidatePKs&gt;&lt;GenerateStatistics collapsed=&apos;1&apos; type=&apos;bool&apos;&gt;&lt;![CDATA[0]]&gt;&lt;/GenerateStatistics&gt;&lt;MaxRejectCount type=&apos;int&apos;&gt;&lt;![CDATA[1]]&gt;&lt;/MaxRejectCount&gt;&lt;/LoadOptions&gt;&lt;/Session&gt;&lt;BeforeAfterSQL collapsed=&apos;1&apos; type=&apos;bool&apos;&gt;&lt;![CDATA[0]]&gt;&lt;/BeforeAfterSQL&gt;&lt;/Usage&gt;&lt;/Properties &gt;'}
            
            SubRecord_Property_Additions={'Database':'/Connection/Database',
                                          'UseSeparateConnectionForTWT':'/Connection/UseSeparateConnectionForTWT',
                                          'DatabaseTWT':'/Connection/UseSeparateConnectionForTWT/Database',
                                          'UsernameTWT':'/Connection/UseSeparateConnectionForTWT/Username',
                                          'PasswordTWT':'/Connection/UseSeparateConnectionForTWT/Password'}
        
    tgt_input_pin=''
    for Record in xml_dict['DSExport'][0]['Job'][0]['Record']:
        if Record['@Identifier']==tgt_record_identifier:
            for Property in Record['Property']:
                if Property['@Name']=='StageType':
                    Property['#text']='NetezzaConnectorPX'
                if Property['@Name']=='InputPins':
                    tgt_input_pin=Property['#text']
            if isinstance(Record['Collection'], dict):
                for SubRecord in Record['Collection']['SubRecord']:
                    #print(SubRecord)
                    SubRecord_Flag=False
                    Property_text=''
                    for Property in SubRecord['Property']:
                        if Property['#text'] in SubRecord_Property_Changes.keys():
                            SubRecord_Flag=True
                            Property_text=Property['#text']
                    if SubRecord_Flag:
                        for Property in SubRecord['Property']:
                            if Property['@Name']=='Value':
                                Property['#text']=SubRecord_Property_Changes[Property_text]
                for item in SubRecord_Property_Additions:
                    curr_SubRecord={'Property': [{'@Name': 'Name', '#text': item}, {'@Name': 'Value', '#text': SubRecord_Property_Additions[item]}]}
                    Record['Collection']['SubRecord'].append(curr_SubRecord)
    
    for Record in xml_dict['DSExport'][0]['Job'][0]['Record']:
        if Record['@Identifier']==tgt_input_pin:
            if isinstance(Record['Collection'], dict):
                for SubRecord in Record['Collection'][0]['SubRecord']:
                    #print(SubRecord)
                    SubRecord_Flag=False
                    Property_text=''
                    for Property in SubRecord['Property']:
                        if Property['#text'] in SubRecord_Property_Changes.keys():
                            SubRecord_Flag=True
                            Property_text=Property['#text']
                    if SubRecord_Flag:
                        for Property in SubRecord['Property']:
                            if Property['@Name']=='Value':
                                Property['#text']=SubRecord_Property_Changes[Property_text]
            elif isinstance(Record['Collection'], list):
                for Collection in Record['Collection']:
                    if isinstance(Collection['SubRecord'],list):
                        for SubRecord in Collection['SubRecord']:
                            #print(SubRecord)
                            SubRecord_Flag=False
                            Property_text=''
                            for Property in SubRecord['Property']:
                                if Property['#text'] in SubRecord_Property_Changes.keys():
                                    SubRecord_Flag=True
                                    Property_text=Property['#text']
                            if SubRecord_Flag:
                                for Property in SubRecord['Property']:
                                    if Property['@Name']=='Value':
                                        Property['#text']=SubRecord_Property_Changes[Property_text]
            
                      
                  
    out_xml = xmltodict.unparse(xml_dict)
    out_xml = out_xml.replace('&amp;', '&')
    
    out_writer = open(out_file,'w')
    out_writer.write(out_xml)

files = os.listdir(in_path)

for file in files:
    if '.xml' in file:
        in_file = in_path+'//'+file
        out_file = out_path+'//'+file.replace('.xml', '_Converted.xml')
        xml_conversion (in_file, out_file)
