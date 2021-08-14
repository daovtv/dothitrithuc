from xlrd import open_workbook
from neo4j import GraphDatabase

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "22686868"))
bookname = 'Thing.xlsx'
sheetname='Person'

wb = open_workbook(bookname)



def print_sheet (s):
    print ('Sheet:',s.name)
    json = {'Schema':'Luocdo'}
    for row in range(s.nrows):
        if row>0:
            col=0
            cparent = 3
            cparent2=4
            if row==1:                
                    json['Object']=s.cell(row,col).value.strip() # ten doi tuong
                    json['Parent']= s.cell(row,cparent).value.strip() # ten doi tuong cha
                    json['Parent2']= s.cell(row,cparent2).value.strip() # ten doi tuong cha 2
                    json['Property']={}
                    json['Rel']= {}
            else:
                col1=8 # ten thuoc tinh
                col2=6 # Property 
                col3=7 # Relation
                if s.cell(row,col2).value!='':
                    json['Property'][s.cell(row,col1).value]=s.cell(row,col2).value.strip() # thuoc tinh la kieu du lieu chuan
                    
                if s.cell(row,col3).value!='':
                    if s.cell(row,col3).value.strip()==json['Object'].strip():
                        json['Property'][s.cell(row,col1).value]=s.cell(row,col3).value.strip() # thuoc tinh tro vao chinh no
                    else:
                        json['Rel'][s.cell(row,col1).value]=s.cell(row,col3).value.strip() # thuoc tinh tro den doi tuong khac
                
                
                    
    return json
def add_object(json):
    label = json['Schema'].strip()
    label2=label.strip()
    name=json['Object'].strip() 
    with driver.session() as session:
        if json['Parent'] == '':
            session.write_transaction(add_node,label,name)
        else:
            session.write_transaction(add_node_rel,label,json['Parent'],'Parent',label,name)
        if json['Parent2'] != '':                
            session.write_transaction(add_node_rel,label,json['Parent2'],'Parent',label,name)    
        if json['Property']!='':
            session.write_transaction(add_property,label,name,json['Property'])
        for n in json['Rel']:
            rel=n
            friend_name=json['Rel'].get(n)
            session.write_transaction(add_node_rel,label,name,rel,label2,friend_name)
def add_tree(json):
    label = 'Tree'
    label2=label.strip()
    name=json['Object'].strip() 
    with driver.session() as session:
        if json['Parent'] == '':
            session.write_transaction(add_node,label,name)
        else:
            session.write_transaction(add_node_rel,label,json['Parent'],'Parent',label,name)
        if json['Parent2'] != '':                
            session.write_transaction(add_node_rel,label,json['Parent2'],'Parent',label,name)    



def add_node(tx,label, name):
    s1 = "MERGE (a:"+label+" {name: " + name + "})"
    print(s1)
    tx.run("MERGE (a:" + label + " {name: $name})",           
           name=name)
def add_node_property(tx,label, names):
    st="{"    
    m = len(names)
    i=0;
    for n in names:
        i = i + 1
        if i < m:
            st=st+ n + ":" + "'"+ names.get(n)+"'"+","            
        else:
            st=st+ n + ":" +"'"+ names.get(n)+"'"    
    st=st+"}"
    print("MERGE (a: " + label + st+")")
    tx.run("MERGE (a:" + label + st+")")
    
def add_property(tx,label,name,Props):
    st=""    
    m = len(Props)
    i=0;
    for n in Props:
        i = i + 1
        if i < m:
            st=st+"p."+ n + "=" + "'"+ Props.get(n)+"'"+","            
        else:
            st=st+"p."+ n + "=" +"'"+ Props.get(n)+"'"    
    
    s1= "MATCH (p:"+label+") WHERE p.name ="+ "'"+name+"'"
    s2 = "Set " + st
    print(s1)
    if st!='':
        print(s2)
    query = (
            "MATCH (p:"+label+") WHERE p.name = $name "
            "Set " + st
        )
    if st!='':
        tx.run(query, name=name)
    
def add_node_friend(tx,label, name, rel, label2,friend_name):    
    s1 = "MERGE (a:"+label+"{name:" + name + "})"
    s2=  "MERGE (a)-[:"+rel+"]->(friend:"+label2+"{name:"+ friend_name+"})"
    print(s1)
    print(s2)
    
    tx.run("MERGE (a:"+label+"{name: $name}) "
           "MERGE (a)-[:"+rel+"]->(friend:"+label2+" {name: $friend_name})",
           name=name, friend_name=friend_name)
def add_node_rel(tx,label, name, rel, label2,friend_name):
    s1=  (
            "Merge (b: "+label2 + " {name: '" + friend_name+ "'}) \n"
            "Merge (a: "+label + " {name: '"+name+"'}) \n"
            "MERGE (a)-[:"+rel+"]->(b)"
        )
    print(s1)
    
    query = (
            "Merge (b: "+label2 + " {name: $friend_name})"
            "Merge (a: "+label + " {name: $name})"
            "MERGE (a)-[:"+rel+"]->(b)"
        )
    tx.run(query, name=name, friend_name=friend_name)
    
def find_node(label, node_name):
        with driver.session() as session:
            result = session.read_transaction(_find_and_return_node, label, node_name)
            for record in result:
                print("Found person: {record}".format(record=record))


def _find_and_return_node(tx, label, node_name):
        query = (
            "MATCH (p:"+label+") "
            "WHERE p.name = $node_name "
            "RETURN p.name AS name"
        )
        result = tx.run(query, node_name=node_name)
        return [record["name"] for record in result]

def doc_workbook():
    
    for s in wb.sheets():                
        js={}
        js=print_sheet(s)    
        add_object(js)

    driver.close()
    print('---- Ket thuc ----')
def doc_book_tree():
    
    for s in wb.sheets():                
        js={}
        js=print_sheet(s)    
        add_tree(js)

    driver.close()
    print('---- Ket thuc ----')    
def doc_sheet():
    sheet = wb.sheet_by_name(sheetname)
    js={}
    js=print_sheet(sheet)    
    add_object(js)

    driver.close()
    print('---- Ket thuc ----')
def doc_in_sheet():
    sheet = wb.sheet_by_name(sheetname)
    js={}
    js=print_sheet(sheet)    
    print(js)
    driver.close()
    print('---- Ket thuc ----')
#----------------------

doc_sheet()

