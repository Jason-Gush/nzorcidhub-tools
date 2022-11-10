# -*- coding: utf-8 -*-
"""
Created on Wed Nov 15 10:26:56 2017

Connects via ODBC to an existing Access database and uses the tables to generate 
an ORCID API v3.0 compliant funding message suitable for upload to the NZ ORCID Hub

@author: Jason
"""

import pyodbc, json

path_to_accdb = r"Full path to the db with \ sep on a Windows machine, e.g., 'C:\Temp\ORCID_store.accdb'"

cnx = pyodbc.connect(r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ='+ path_to_accdb + ';')
ptdBase = cnx.cursor()

# org_files_to_make = ("AUT", "CCH", "GNS", "MAU", "MIM", "NIW", "PAF", "UOA", "UOC", "UOO", "UOW", "VUW")
# for use to split results by org if task sizes would otherwise be too big.
org = "OTH"

string_replacements = [("Maori", "Māori"), (" va ", " vā "), (" va,", " vā,"), ("Va", "Vā"), ("Tangata", "Tāngata"),
                       ("matauranga", "mātauranga"), ("Matauranga", "Mātauranga"), ("Taupo", "Taupō"),
                       ("Pakeha", "Pākehā "), ("Rangitahua", "Rangitāhua"), ("Ngati", "Ngāti"), ("Whanau", "Whānau"),
                       ("whanau", "whānau"), ("ß", "β"), ("Otautahi", "Ōtautahi"), ("wahine", "wāhine"),
                       ("wähine", "wāhine"), ("?133p53", "Δ133p53"), ("Kaikoura", "Kaikōura"),
                       ("‘Alala (Hawai‘i)", "‘Alalā (Hawai‘i) "), ("Kakapo", "Kākāpō"), ("hapu", "hapū"),
                       (" pa ", " pā "), ("Rongoa", "Rongoā"), ("Otorohanga", "Ōtorohanga")]

contract_query_str = 'SELECT [Proposal], [Contract], [Project], [Abstract], [Funding], [StartYear], [FinishYear]  FROM [Contract];'

team_query_str = "SELECT [Person], [FirstName], [Surname], [Name], [EMail], [Order], [Role]" \
                 "FROM [Contract_Team] " \
                 "WHERE ((([Contract_Team].[Contract])=?))" \
                 "ORDER BY [Order];"

externalids_query_str = 'SELECT DISTINCT [Contract] FROM [Contract] WHERE [Proposal] = ?;'


contract_rows  = ptdBase.execute(contract_query_str)

contracts = []
funding = []
contract_count = 0
investigator_count = 0

contract_columns = [column[0] for column in contract_rows.description]
team_columns = ['Person', 'FirstName', 'Surname', 'Name', 'Email', 'Order', 'Role']

for contract in contract_rows:
    contracts.append(dict(zip(contract_columns, contract)))

for contract in contracts:
        
    print(contract['Proposal'])
    team = []
    invitees = []
    contributors = []
    external_ids = []
    to_write = 0

    # uncomment for Marsden
    #category = 'Standard'
    #if(contract['Category']=="FS"):
        #category = 'Fast-Start'

    # make the invitee and contributor elements from the named team members
    names = ptdBase.execute(team_query_str, contract['Contract'])
    for name in names:
        team.append(dict(zip(team_columns, name)))
    for person in team:
        if person['Email'] is not None:
            invitees.append({"identifier":person['Person'],
                                "email": person['Email'],
                                "first-name": person['FirstName'],
                                "last-name": person['Surname']})
            to_write = 1
            investigator_count += 1

        contributors.append({"credit-name":{"value":person['Name']},
                             "contributor-attributes":{"contributor-role":person['Role']}
                             })

    external_ids.append({"external-id-type":"proposal-id", "external-id-value":contract['Proposal'], "external-id-relationship":"self"})

    contract_ids = ptdBase.execute(externalids_query_str, contract['Proposal'])
    for contract_id in contract_ids:
        external_ids.append({"external-id-type":"grant_number", "external-id-value":contract_id[0], "external-id-relationship":"self"})
    if to_write == 1:
        contract_count += 1

        #catch where unicode has been lost in Access
        for pair in string_replacements:
            contract['Abstract'] = contract['Abstract'].replace(pair[0], pair[1])    

        funding.append({"invitees":invitees,
                        "type" : "CONTRACT",
                        "title":{"title":{"value":contract['Project']}},
                        "short-description":contract['Abstract'],
                        "amount":{"value":str(int(contract['Funding'])), "currency-code":"NZD"},
                        "organization-defined-type":{"value":"Royal Society Te Apārangi Funding Programme"},
                        "start-date":{"year":{"value":str(int(contract['StartYear']))}},
                        "end-date":{"year":{"value":str(int(contract['FinishYear']))}},
                        "external-ids":{"external-id":external_ids},
                        "contributors":{"contributor":contributors},
                        "organization":{
                                "name":"Royal Society Te Apārangi",
                                "address":{
                                        "city":"Wellington",
                                        "country":"NZ"},
                                "disambiguated-organization":{
                                        #Generic Royal Society Idenifier
                                        "disambiguated-organization-identifier":"http://dx.doi.org/10.13039/501100001509",
                                        "disambiguation-source":"FUNDREF"}
                                }
                        })

with open(r"C:\Temp\Funding_hub_jsons_" + org + ".json", "w", encoding='utf-8') as outfile:
    data = json.dumps(funding, ensure_ascii=False)
    outfile.write(data)

print("Org " + org)
print("Number of contracts written " + str(contract_count))
print("Number of investigators written " + str(investigator_count))

cnx.close()