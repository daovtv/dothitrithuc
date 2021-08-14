from neo4j import GraphDatabase
class Graph:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri,auth=(user, password))
    def close(self):
        self.driver.close()
    def add_node(self,label, name):
        with self.driver.session() as session:
            session.write_transaction(self._add_node,label,name)            

    @staticmethod    
    def _add_node(tx,label, name):
        s1 = "MERGE (a:"+label+" {name: " + "'"+ name + "'" + " })"
        print(s1)
        tx.run("MERGE (a:" + label + " {name: $name})",name=name)

    def add_object(self,json):
        label = json['Schema'].strip()
        label2=label.strip()
        name=json['Object'].strip() 
        with self.driver.session() as session:
            if json['Parent'] == '':
                session.write_transaction(add_node,label,name)
            else:
                session.write_transaction(add_node_rel,label,json['Parent'],'Parent',label,name)
            if json['Property']!='':
                session.write_transaction(add_property,label,name,json['Property'])
            for n in json['Rel']:
                rel=n
                friend_name=json['Rel'].get(n)
                session.write_transaction(add_node_rel,label,name,rel,label2,friend_name)

           
    def add_properties_of_node(tx,label, names):
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
    def add_property(self,label,name,Props):
        with self.driver.session() as session:
            session.write_transaction(self._add_property,label,name, Props)
    @staticmethod
    def _add_property(tx,label,name,Props):
        st=""
        m = len(Props)
        i=0;
        for n in Props:
            i = i + 1                           
            if i < m:
                if type(Props.get(n))=="<class 'int'>":
                    st=st+"p."+ n + "=" + str(Props.get(n)) + ","
                else:
                    st=st+"p."+ n + "=" + "'"+ str(Props.get(n))+"'"+","
            else:
                if type(Props.get(n))=="<class 'int'>":
                    st=st+"p."+ n + "=" + str(Props.get(n))   
                else:
                    st=st+"p."+ n + "=" +"'"+ str(Props.get(n))+"'"    
    
        s1= "MATCH (p:"+label+") WHERE p.name ="+ "'" + name +"'"
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
    def add_node_friend(self,label, name, rel, label2,friend_name):
        with self.driver.session() as session:
            session.write_transaction(self._add_node_friend(label, name, rel, label2,friend_name))
    @staticmethod
    def _add_node_friend(tx,label, name, rel, label2,friend_name):    
        s1 = "MERGE (a:"+label+"{name:" + name + "})"
        s2=  "MERGE (a)-[:"+rel+"]->(friend:"+label2+"{name:"+ friend_name+"})"
        print(s1)
        print(s2)
    
        tx.run("MERGE (a:"+label+"{name: $name}) "
           "MERGE (a)-[:"+rel+"]->(friend:"+label2+" {name: $friend_name})",
            name=name, friend_name=friend_name)


    def add_node_rel(self, label, name, rel, label2, friend_name):        
        with self.driver.session() as session:
            session.write_transaction(self._add_node_rel,label, name, rel, label2, friend_name)
    @staticmethod
    def _add_node_rel(tx, label, name, rel, label2, friend_name):
        print()
        s1= ("Merge (b: "+label2 + " {name: '"+friend_name+"'}) \n"
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
    def find_prop_and_rel(self,label,name, prop):
        timco = False
        level=0
        childobj = ''
        result = self.find_prop_rel_node(label,name)
        # tim trong tap thuoc tinh
        if len(result)>0 and prop in result[0][0]:
            timco = True
            level = 1 # tap thuoc tinh attribute trong nut hien hanh
            
        # tiep tuc tim trong cac cung 
        d=len(result)
        for i in range(0,d):
            if prop in result[i][1]:
                if not timco:
                    timco=True
                    level = 2 # thuoc tinh la nhan cua cung
                    childobj = result[i][2]
                else:
                    level = 4 # thuoc tinh vua la attribute vua la nhan cung
                    childobj = result[i][2]
                if timco:
                    break

        ketqua=[]
        ketqua.append(level)
        ketqua.append(childobj)
        return ketqua

    def check_prop2(self,obj_name, prop):
        label='Luocdo'
        name=obj_name
        timco = False
        level=0
        childobj = ''
        #kiem tra nut co ton tai
        kq=self.find_node(label,name)        
        if kq==[]:
            print('Nut khong co trong do thi !')
            return [0,'']
        # nut hien hanh co phai nut la
        result = self.find_childs_node(label,name)   
        
        if len(result) > 0:            
            print("khong phai nut la ")
            kq=self.find_prop_and_rel(label,name,prop)
            #print(kq[0])
            level=kq[0] # =1,2 or 4-> pro, rel hoac ca hai 
            childobj=kq[1]
            #print(level)
            if level>0:
                timco=True
        else: # la nut la
            result = self.find_prop_node(label, name)
            # tim trong tap thuoc tinh nut la, khong tim cung
            if len(result)>0 and prop in result[0]:
                timco = True
                level = 1 # tap thuoc tinh nut hien hanh
        # tim trong cac nut cha
        if not timco:
            print("tim cac nut cha cua ",name)
            result=self.find_parents_node(label,name)            
            print(result)
            qu=[]
            qu.append(result)
            while qu!=[] and not timco:
                result=qu.pop(0)
                for i in range(0,len(result)): # co the có hai nut cha                    
                    kq=self.find_prop_and_rel(label,result[i],prop)
                    level=kq[0]
                    childobj=kq[1]
                    if level==1:
                        level=3 # la prop cua nut cha
                    if level==2:
                        level=5 # la cung cua nut cha
                    if level==4:
                        level=7 # vua la prop vua la cung
                    if level>0:
                        timco=True
                        print(result[i])
                        break
                if not timco and len(result)>0:
                    result=self.find_parents_node(label,result[i])
                    print(result)
                    qu.append(result)
                        
        ketqua=[]
        ketqua.append(level)
        ketqua.append(childobj)
        #print(ketqua)
        return ketqua
    
    def check_prop(self,obj_name, prop):
        label='Luocdo'
        name=obj_name
        timco = False
        level=0
        childobj = ''
        # nut hien hanh co phai nut la
        result = self.find_childs_node(label,name)   
        
        if len(result) > 0: # khong phai nut la
            print(name, ' khong phai la nut la ')
            result = self.find_prop_rel_node(label,name)
             # tim trong tap thuoc tinh
            if len(result)>0 and prop in result[0][0]:
                timco = True
                level = 1 # tap thuoc tinh attribute trong nut hien hanh
            # tiep tuc tim trong cac cung 
            #if not timco:
            d=len(result)
            for i in range(0,d):
                if prop in result[i][1]:
                    if not timco:
                        timco=True
                        level = 2 # thuoc tinh la nhan cua cung
                        childobj = result[i][2] # nút mà cung tro den
                    else:
                        level = 4 # thuoc tinh vua la attribute vua la nhan cung
                        childobj = result[i][2]
                    if timco:
                        break
        else: # la nut la
            print(name, ' La nut la ')
            result = self.find_prop_node(label, name)
            # tim trong tap thuoc tinh nut la, khong tim cung
            if len(result)>0 and prop in result[0]:
                timco = True
                level = 1 # tap thuoc tinh nut hien hanh

        # tim trong cac nut cha
        if not timco:
            # Tìm property trong nút cha gần nhất
            result = self.find_prop_parent_node(label,name, "Parent")
            # tìm trong các cung của nút cha gần nhất
            
            if len(result)>0 :
               for i in range(0,len(result)): # co the có hai nut cha
                  if prop in result[i]:
                      timco=True
                      level = 3 # nut cha
                      break
            else:
                # không có, nên tìm trong các nút cha cao hơn
                while result!=[]: # nếu chưa đến đỉnh thì chắc chắn result <> []
                    name = result[0]['name'] # lấy tên nút cha
                    # tìm nút cha cao hơn và lặp lại quá trình
                    result = self.find_prop_parent_node(label,name, "Parent")            
                    if len(result)>0 :
                        for i in range(0,len(result)): # co the có hai nut cha
                          if prop in result[i]:
                              timco=True
                              level = 3 # nut cha
                              break
            
        ketqua=[]
        ketqua.append(level)
        ketqua.append(childobj)
        #print(ketqua)
        return ketqua
    def find_node(self, label, node_name):
        with self.driver.session() as session:
            result = session.read_transaction(self._find_node, label, node_name)
        return result
    def find_prop_node(self, label, node_name):
        with self.driver.session() as session:
            result = session.read_transaction(self._find_and_return_node, label, node_name)
        return result

    def find_prop_rel_node(self, label, node_name):
        with self.driver.session() as session:            
            result = session.read_transaction(self._find_and_return_node3, label, node_name)
        return result
    

    def find_prop_parent_node(self, label, node_name,rel):
        with self.driver.session() as session:
            result = session.read_transaction(self._find_and_return_parent_node, label, node_name, rel)
        return result

    def find_childs_node(self, label, node_name):
        with self.driver.session() as session:
            result = session.read_transaction(self._find_and_return_childs, label, node_name)
        return result
    def find_parents_node(self, label, node_name):
        with self.driver.session() as session:
            result = session.read_transaction(self._find_and_return_parents, label, node_name)
        return result
    @staticmethod
    def _find_and_return_parent_node(tx, label, node_name, rel):
        query = (
            "MATCH (p:"+label +")-[:"+rel+"]->(q:"+label+") "
            "WHERE q.name = $node_name "
            "RETURN properties(p) as key"
        )        
        result = tx.run(query, node_name=node_name)
        l=[]
        for record in result:            
            l.append(record[0])

        return l
    @staticmethod
    def _find_node(tx, label, node_name):
        query = (
            "MATCH (p:"+label+") "
            "WHERE p.name = $node_name "
            "RETURN p.name"
        )
        result = tx.run(query, node_name=node_name)
        l=[]
        for record in result:            
            l.append(record[0])

        return l
    @staticmethod
    def _find_and_return_node(tx, label, node_name):
        query = (
            "MATCH (p:"+label+") "
            "WHERE p.name = $node_name "
            "RETURN properties(p) as key"
        )
        result = tx.run(query, node_name=node_name)
        l=[]
        for record in result:            
            l.append(record[0])

        return l
    @staticmethod
    def _find_and_return_node2(tx, label, node_name):
        query = (
            "MATCH (p:"+label+")-[r]->() "
            "WHERE p.name = $node_name "
            "RETURN properties(p) as prop, collect(type(r)) as type"
        )
        result = tx.run(query, node_name=node_name)
        #print(result)
        l=[]
        
        for record in result:
             l.append(record[0])
             l.append(record[1])
            
        return l
    @staticmethod
    def _find_and_return_node3(tx, label, node_name):
        query = (
            "MATCH (p:"+label+")-[r]->(q) "
            "WHERE p.name = $node_name "
            "RETURN properties(p) as prop, collect(type(r)) as type, q.name as q"
        )
        result = tx.run(query, node_name=node_name)
        #print(result)
        l=[]
        i=0
        for record in result:
             g=[]
             l.append(g)
             l[i].append(record[0])
             l[i].append(record[1])
             l[i].append(record[2])
             i=i+1
            
        return l
    @staticmethod
    def _find_and_return_childs(tx, label, node_name):
        query = (
            "MATCH (p:"+label+")-[r]->(q) "
            "WHERE p.name = $node_name "
            "RETURN q.name "
        )
        result = tx.run(query, node_name=node_name)
        l=[]
        for record in result:            
            l.append(record[0])
        return l
    @staticmethod
    def _find_and_return_parents(tx, label, node_name):
        if label=="Luocdo":
            query = (
                "MATCH (p)-[r:Parent]->(q:"+label+") "
                "WHERE q.name = $node_name "
                "RETURN p.name "
            )
        else:
            query = (
                "MATCH (p)-[r]->(q:"+label+") "
                "WHERE q.name = $node_name "
                "RETURN p.name "
            )
        result = tx.run(query, node_name=node_name)
        l=[]
        #print(query)
        for record in result:            
            l.append(record[0])
            
        return l
    def danh_sach_loai_doi_tuong(self):
        with self.driver.session() as session:
            result = session.read_transaction(self._danh_sach_loai_doi_tuong)
        return result
    @staticmethod
    def _danh_sach_loai_doi_tuong(tx):
        label = 'Tree'
        query = (
                "MATCH (p:"+label+") "                
                "RETURN p.name "
            )
        result = tx.run(query)
        l=[]
        #print(query)
        for record in result:            
            l.append(record[0])            
        return l
    def danh_sach_thuoc_tinh(self,loai):
        with self.driver.session() as session:
            result = session.read_transaction(self._danh_sach_thuoc_tinh,loai)
        return result
    @staticmethod
    def _danh_sach_thuoc_tinh(tx, loai):
        label = 'Luocdo'
        query = (
                "MATCH (p:"+label+") "
                "Where p.name=$loai   "
                "RETURN keys(p) "
            )
        result = tx.run(query,loai=loai)
        for rec in result:
            l=rec[0] 
        #print(l)
        return l
    def danh_sach_thuc_the(self,loai):
        with self.driver.session() as session:
            result = session.read_transaction(self._danh_sach_thuc_the,loai)
        return result
    @staticmethod
    def _danh_sach_thuc_the(tx, loai):
        label = 'Data:'+loai
        query = (
                "MATCH (p:"+label+") "                
                "RETURN p.name, p.Ten "
            )
        result = tx.run(query,loai=loai)
        l=[]
        i=0
        for rec in result:
            g=[]
            l.append(g)
            l[i].append(rec[0])
            l[i].append(rec[1])
            i=i+1
            #print(rec)
        return l
    def key_value(self,loai,mten):
        with self.driver.session() as session:
            result = session.read_transaction(self._key_value,loai,mten)
        return result
    @staticmethod
    def _key_value(tx, loai,mten):
        label = 'Data:'+loai
        query = (
                "MATCH (p:"+label+") "
                "Where p.name=$mten   "
                "RETURN properties(p) "
            )
        result = tx.run(query,mten=mten)
        for rec in result:
            l=rec[0] 
        #print(l)
        return l
    def URL_image(self,loai,mten):
        with self.driver.session() as session:
            result = session.read_transaction(self._URL_image,loai,mten)
        return result
    @staticmethod
    def _URL_image(tx, loai,mten):
        label = 'Data:'+loai
        query = (
                "MATCH (p:"+label+") -[:URL_ảnh]-> (q) "
                "Where p.name=$mten   "
                "RETURN q.name "
            )
        result = tx.run(query,mten=mten)
        l=''
        for rec in result:
            l=rec[0] 
        #print(query)
        return l
