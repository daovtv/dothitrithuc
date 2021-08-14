from Excel2 import Excel
from Graph import Graph
graph = Graph("bolt://localhost:7687", "neo4j", "22686868")
#excel = Excel()
# Nếu prop hop le tra ve True
def doc_danh_muc(filename,sheetname = 'Sheet1',row=1,col=1):
    ex = Excel(filename)
    ex.open_workbook()
    sheet = ex.select_sheet(sheetname)
    json = {}
    data = []
    print('sheet = ',sheetname)
    nrow = ex.nrow(sheet,row)
    ncol = ex.ncol(sheet,col)
    print('row = ', nrow)
    print('col = ',ncol)
    n=-1
    ghi=False
    for i in range(2,nrow):
       for j in range(1,ncol):
            if type(sheet.cell(i,j).value)==type(None):
                json[sheet.cell(1,j).value]= ""
            else:
                json[sheet.cell(1,j).value]= sheet.cell(i,j).value
       print()
       print('Dữ liệu excel: ')
       print(json)
       data.append(json)
       n = n + 1       
       ketqua = graph.check_prop2(data[n]['Object'],data[n]['Property'])
       #print(ketqua) 
       level=ketqua[0]
       rel_obj = ketqua[1]
       if level ==0:
           print('Property: ' ,data[n]['Property'],': khong co trong Luoc do')
           sheet.cell(i,ncol).value= "False"
           ghi = True
       if level==1 or level ==3 or (level==7 and data[n]['Content'][0:3].upper()!='OBJ'):
           label = data[n]['Object']
           name = data[n]['Name_ID']
           prop={}
           prop[data[n]['Property']]=data[n]['Content'] # prop là Attribute của nut           
           graph.add_node(label+':Data',name)
           graph.add_property(label,name,prop)
       if level==5 or (level==7 and data[n]['Content'][0:3].upper()=='OBJ'):           
            label = data[n]['Object']+':Data'
            name = data[n]['Name_ID']
            rel = data[n]['Property'] # prop la cung
            label2 = rel_obj +':Data' # tro den doi tuong
            arr_name2 = data[n]['Content'].split(' ')
            l=len(arr_name2)
            name2 = arr_name2[l-1]
            
            graph.add_node_rel(label,name,rel,label2,name2)
    if ghi:
        ex.save(filename)    
    return


    
def doc_excel():
    doc_danh_muc('Mau import du lieu.xlsx', 'Sheet1') # URL, PostalAddress, Place_ditich, Place, PostalAddress2, URL2

doc_excel()
graph.close()




    
   

