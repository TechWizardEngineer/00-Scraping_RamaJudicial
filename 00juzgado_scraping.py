pip list
print(list)

pip install PyDrive
print("pydrive installed")

from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
# from google.colab import auth
# auth.authenticate_user()
 
import gspread
from oauth2client.client import GoogleCredentials
gc = gspread.authorize(GoogleCredentials.get_application_default())

# #from google.colab import auth
# #auth.authenticate_user()

# from pydrive.auth import GoogleAuth
# from pydrive.drive import GoogleDrive
# from google.colab import auth
# from oauth2client.client import GoogleCredentials
from gspread_dataframe import get_as_dataframe, set_with_dataframe

# # Authenticate and create the PyDrive client.
# auth.authenticate_user()


import os
base_dir_drive = "/content/drive/My Drive/Colab Notebooks/My_projects/00-Scraping_RamaJudicial/"
base_dir_gdrive = "/content/gdrive/My Drive/Colab Notebooks/My_projects/00-Scraping_RamaJudicial/"

gauth = GoogleAuth()

# Try to load saved client credentials
gauth.LoadCredentialsFile("mycreds.txt")

drive = GoogleDrive(gauth)

if gauth.credentials is None:
    # Authenticate if they're not there

    # This is what solved the issues:
    gauth.GetFlow()
    gauth.flow.params.update({'access_type': 'offline'})
    gauth.flow.params.update({'approval_prompt': 'force'})

    gauth.LocalWebserverAuth()

elif gauth.access_token_expired:

    # Refresh them if expired

    gauth.Refresh()
else:

    # Initialize the saved creds

    gauth.Authorize()

# Save the current credentials to a file
gauth.SaveCredentialsFile("mycreds.txt")  

drive = GoogleDrive(gauth)

#print(base_dir_drive)
#print(base_dir_gdrive)

# Libraries needed for basic web-scraping
from IPython.core.display import HTML
from bs4 import BeautifulSoup
from IPython.display import IFrame
import urllib3 # package required to interact with live webpage
import pandas as pd # will use to store the data from the webpage
import time

# Steps to make:
# 1. Getting html from webpage with urllib 
# 2. Save it in .html in order not to block the website for excessive scraping
# 3. Parse the .html with Beatiful Soupt#!pip install -U -q PyDrive

# Root Configurations
project_dir = "project/datalab-colab-jupyter/"
data_dir = "project/data/"

#import gspread
#from oauth2client.client import GoogleCredentials

#gc = gspread.authorize(GoogleCredentials.get_application_default())

#print(base_dir + data_dir+ 'input/db_juzgados')
#worksheet_sheet1 = gc.open(base_dir + data_dir + 'input/db_juzgados').worksheet("00ActiveOrganismoJudicial")
#worksheet_sheet2 = gc.open(base_dir + data_dir + 'input/db_juzgados').sheet2

#!pip install unicode


"""## Functions that would eventually go to classes (Monolithic Solution) to use in different sessions"""

# Loading sheets from GoogleSheets in GoogleDrive
def f0_load_sheets_from_source(source_url,namesheet1,namesheet2):
  """Gets the positive result of a change in status of law processes

    Parameters
    ----------
    source_url : url address
        Link to GoogleSheets to parse as db_juzgados
    json_name : str
        Name of the JSON to be created
    destination : str
        PATH in which the created HTML is going to be saved

    Returns
    -------
    df_act_juzgados : str
        A message saying that process passed without issues

    df_act_subjuzgados : str
        A message saying that process passed without issues

    prints : Different prints
        Differents prints of the processing for loading sheets
  """
  db_juzgados = gc.open_by_url(source_url)
  main_sheet = db_juzgados.worksheet(namesheet1)
  juzgados_sheet = db_juzgados.worksheet(namesheet2)
  df_act_juzgados = get_as_dataframe(main_sheet)
  df_act_juzgados = df_act_juzgados.loc[:,~df_act_juzgados.columns.str.match("Unnamed")]

  df_act_subjuzgados = get_as_dataframe(juzgados_sheet)
  df_act_subjuzgados = df_act_subjuzgados.loc[:,~df_act_subjuzgados.columns.str.match("Unnamed")]

  #dict_df_query = {}
  #for tab in query_list:
  #  dict_df_query[tab] = pd.read_excel(io.BytesIO(io_query),sheet_name=tab)
  #return(dict_df_query)

  print(df_act_juzgados.dtypes)
  print("\n")
  print(df_act_juzgados.columns)
  print("\n")
  print(df_act_juzgados.shape)
  print("\n")
  print(type(df_act_juzgados))

  print(df_act_subjuzgados.dtypes)
  print("\n")
  print(df_act_subjuzgados.columns)
  print("\n")
  print(df_act_subjuzgados.shape)
  print("\n")
  print(type(df_act_subjuzgados))

  return(df_act_juzgados,df_act_subjuzgados)

def f4_generating_merge_subjuzgados_report(dfbyreport,user_name,destination):
  
  """Gets the positive result of a change in status of law processes

    Parameters
    ----------
    dfbyreport: dataframe 
        Dataframe of all scrape juzgados sort by Fecha Actualizacion
    user_name : str
        Name of client
    destination : str
        PATH of folder in which file should be written in

    Returns
    -------
    df_act_juzgados : str
        A message saying that process passed without issues

    df_act_subjuzgados : str
        A message saying that process passed without issues

    prints : Different prints
        Differents prints of the processing for loading sheets
  """

  # Import Drive API and authenticate.
  from google.colab import drive
  from datetime import date
  from datetime import datetime

  drive.mount("/content/drive",force_remount=True)
  root_dir = "/content/drive/My Drive/"

  date_execution_report = str(date.today())
  csv_name = "reporte_"+user_name+"_"+date_execution_report

  try:
    # Write the DataFrame to CSV file.
    #with open(destination+csv_name+".xlsx", 'w') as f:
    #  dfbyreport.to_excel(f)
    dfbyreport.to_excel(destination+csv_name+".xlsx",sheet_name = user_name+"_"+date_execution_report,index=False)
  except Exception as e:
    print(e)

  #df.to_csv('data.csv')
  #!cp data.csv "drive/My Drive/"
def structuring_sending_options():
  pass

def f1_checking_scraping_status(url):

  """Gets status of scraping status for a URL

    Parameters
    ----------
    url : str
        The URL of the ramajudicial web page to be scraped
    Returns
    -------
    site_content: html
        Site content in .html to be scraped
    mssg: str
        A message saying that is ok to scrape or not
  """
  http = urllib3.PoolManager()
  urlTemp = url
  resp = http.request('GET',urlTemp)
  site_content = resp.data.decode('utf-8')

  
  if(resp.status==200):
    mssg = "It is ok to do scraping"
  else:
    mssg = "We may need another idea"
  return(site_content,mssg)

def f2_writing_html_being_scraped(site_content,html_name, destination):
  
  """Writes a html file from the webpag to be scraped

    Parameters
    ----------
    site_content : str
        HTML object to be scraped with BeatifulSoup
    html_name : str
        Name of the HTML to be created
    destination : str
        PATH in which the created HTML is going to be saved
    Returns
    -------
    mssg : str
        A message saying that process went smoothly
  """
  try:
      with open(destination+html_name,"w") as f:
        f.write(destination + site_content)
  except Exception as e:
    print(e)


def f3aux_generating_date_from_extracted_text(daytemp, monthtemp):
  #from unicodedata import normalize
  #normalize('NFKD', word)
  #import unicode
  from datetime import datetime
  from unicodedata import normalize
  
  # Making change in daytemp if apply
  #daytemp = normalize('NFKD',str(daytemp))
  daytemp = str(daytemp).strip()
  monthtemp = str(monthtemp).strip()
  yeartemp = str(datetime.today().year)
  #string_date = str(datetime.today().year)+"-"+str(month_number_temp).strip()+"-"+dayextracted
  date_string = yeartemp+"/"+monthtemp+"/"+daytemp
  date_transform_temp = datetime.strptime(date_string, "%Y/%m/%d")
  
  return(date_transform_temp,[daytemp,monthtemp,yeartemp])
  #dayextracted = str(atagitem.text).replace(u'\xa0', u' ')
        #string_date = str(datetime.today().year)+"-"+str(month_number_temp).strip()+"-"+dayextracted
        #print(type(atagitem.text),month_number_temp.strip(),type(str(datetime.today().year)))

def f3aux_normalizing_href(hrefinput):

  href = hrefinput

  if(href.startswith("/documents/")):
    #print(hrefinput+" To normalize point1\n")
    href_normalize = "https://www.ramajudicial.gov.co"+str(href)
  else:
    href_normalize = str(href)
  
  #print(href_normalize+" To normalize point2\n")
  return(href_normalize)
        

def f3_getting_positive_df_from_webpage(site_content,namesubjuzgado):

  """Gets the positive result of a change in status of law processes

    Parameters
    ----------
    site_content: html
        Site content in .html to be scraped
    namesubjuzgado: str
        Name of subjuzgado to be inserted in dataframe to send email.

    Returns
    -------
    df_final : pd.Dataframe
        Final dataframe with the dates in which href is present
  """

  from datetime import datetime
  import unicodedata

  #clean_text = BeautifulSoup(raw_html, "lxml").text
  #print clean_text
  #u'Dear Parent,\xa0This is a test message,\xa0kindly ignore it.\xa0Thanks'
  
  #new_str = unicodedata.normalize("NFKD",clean_text)
  #print new_str

  # Use html.parser to create soup
  s = BeautifulSoup(site_content1, 'html.parser')
  #sclean = unicodedata.normalize("NFKD",s)

  search1 = s.find_all('div', {'class':'aui-tabview-content-item'})
  list_months = ["Ene","Feb","Mar",
  "Abr","May","Jun",
  "Jul","Ago","Sep",
  "Oct","Nov","Dic"]
  list_months_numbers = ["01","02","03",
  "04","05","06",
  "07","08","09",
  "10","11","12"]
  list_dfs_months = []
  count_months_with_table = 0
  namesubjuzgado = namesubjuzgado

  for idx,html_element in enumerate(search1):
    if(html_element.find(['table'])):
      dfbymonth = pd.DataFrame(columns=('Tipo_Reporte','Nombre Juzgado','Mes','Hubo_Actualizaciones', 'Fecha_Actualizacion', 'PDF_Estados'))
      print("\n Month",list_months[idx],"has table")
      count_months_with_table +=1
      #print(item.prettify())
      month_name_temp = list_months[idx]
      month_number_temp = list_months_numbers[idx]
      searchTemp = html_element.find_all('a',href=True)

      for idx,atagitem in enumerate(searchTemp):
      #print("True",atagitem.text,atagitem['href'],atagitem.prettify())
      #print("True",atagitem.text,atagitem['href'])
        #dayextracted = str(atagitem.text).replace(u'\xa0', u' ')
        #string_date = str(datetime.today().year)+"-"+str(month_number_temp).strip()+"-"+dayextracted
        #print(type(atagitem.text),month_number_temp.strip(),type(str(datetime.today().year)))
        #date_temp = datetime.strptime(string_date, "%Y-%m-%d")
        hrefinput = atagitem['href']
        month_name_temp = str(month_name_temp)
        date_string,templist = f3aux_generating_date_from_extracted_text(atagitem.text,month_number_temp)
        href_normalize = f3aux_normalizing_href(hrefinput)
        dfbymonth.loc[idx] = ['Estados',namesubjuzgado,month_name_temp,"Si",date_string,href_normalize]

      dfbymonth = dfbymonth.sort_values(by="Fecha_Actualizacion",ascending=False)
      list_dfs_months.append(dfbymonth)
      #print(dfbymonth)
  #dfbymonth.dtypes
  #print(search1.prettify())
  print("\nToday's date is",datetime.now())
  #print("\nThe number of months with",count_months_with_table)
  df_final = pd.concat(list_dfs_months)
  return(df_final)

def f4_writing_jsonfile(json_final,json_name,destination):

  """Gets the positive result of a change in status of law processes

    Parameters
    ----------
    json_final : .json
        JSON object to be writing in a specific destination
    json_name : str
        Name of the JSON to be created
    destination : str
        PATH in which the created HTML is going to be saved

    Returns
    -------
    mssg : str
        A message saying that process passed without issues
  """

  # Output as Json
  with open(destination+json_name,"w") as f:
    json.dump(destination+json_final,f)

URL = "https://docs.google.com/spreadsheets/d/1MhVtLKsSr42jlesrTPa7nrN7Pn5wurxkjSKe77oiHvQ/edit?usp=sharing"

def f3aux_conditions_after_tablev0(html_element,list_months,list_months_numbers,idx):
  #Para escenario de: https://www.ramajudicial.gov.co/web/juzgado-001-promiscuo-municipal-de-puerto-nare/71
  
  dfbymonth = pd.DataFrame(columns=('Tipo_Reporte','Nombre Juzgado','Mes','Hubo_Actualizaciones', 'Fecha_Actualizacion', 'PDF_Estados'))
  print("\n Month",list_months[idx],"has table")
  #count_months_with_table +=1
  #print(item.prettify())
  month_name_temp = list_months[idx]
  month_number_temp = list_months_numbers[idx]
  searchTemp = html_element.find_all('a',href=True)
  list_dfs_months = []
  
  for idx,tag_item in enumerate(searchTemp):
    hrefinput = tag_item['href']
    month_name_temp = str(month_name_temp)
    date_string,templist = f3aux_generating_date_from_extracted_text(tag_item.text,month_number_temp)
    href_normalize = f3aux_normalizing_href(hrefinput)
    dfbymonth.loc[idx] = ['Estados',namesubjuzgado,month_name_temp,"Si",date_string,href_normalize]
    dfbymonth = dfbymonth.sort_values(by="Fecha_Actualizacion",ascending=False)
    list_dfs_months.append(dfbymonth)
  
  return(list_dfs_months)

def f3change_getting_positive_df_from_webpage(site_content,namesubjuzgado):
    from datetime import datetime
    import unicodedata
    s = BeautifulSoup(site_content1, 'lxml')
    #sclean = unicodedata.normalize("NFKD",s)
    search1 = s.find_all('div', {'class':'aui-tabview-content-item'})
    list_months = ["Ene","Feb","Mar",
    "Abr","May","Jun",
    "Jul","Ago","Sep",
    "Oct","Nov","Dic"]
    list_months_numbers = ["01","02","03",
    "04","05","06",
    "07","08","09",
    "10","11","12"]

    count_months_with_table = 0
    namesubjuzgado = namesubjuzgado
    
    for idx,html_element in enumerate(search1):
      if(html_element.find(['table'])):
        list_dfs_months = f3aux_conditions_after_tablev0(html_element,list_months,list_months_numbers,idx)
        #print(dfbymonth)
    #dfbymonth.dtypes
    #print(search1.prettify())
    print("\nToday's date is",datetime.now())
    #print("\nThe number of months with",count_months_with_table)
    
    df_final = pd.concat(list_dfs_months)
    
    return(df_final)

# Loop over functions, Backup
list_urlsubjuzgados = []

for indexurl in range(3):
  name_tiporeporte = "Estados"
  namesubjuzgado = df_act_subjuzgados["Nombre_JPMuni"].iloc[indexurl]
  urltemp = df_act_subjuzgados["URL2"].iloc[indexurl]

  if(urltemp != None ):
    print("\n Se esta corriendo para",indexurl, namesubjuzgado)
    #if(indexurl != 1 and indexurl !=4):
    site_content1, mssg1 = f1_checking_scraping_status(urltemp)
    #f2_writing_html_being_scraped(urltemp,"jcivilmuni_honda1.html",base_dir+data_dir)
    df_temp = f3change_getting_positive_df_from_webpage(site_content=site_content1,namesubjuzgado=namesubjuzgado)
    
    #(site_content,namesubjuzgado)
    print(df_temp)
    list_urlsubjuzgados.append(df_temp)

df_final_urlsubjuzgados = pd.concat(list_urlsubjuzgados)

df_final_urlsubjuzgados

"""### Generating .csv after getting last concat of dataframe to email"""

#df_final_urlsubjuzgados.to_csv()

#P1:Validaciones bajo la muestra de juzgados resaltados (Tipo Reportes => Estados (Para hoy enfocados en Estados) y Traslados (Manana se valida con Traslados)):
# -Se entendio que toca hacer escenarios para pagina de estados de Juzgados dentro del conjunto de muestras en serie 2
#P2:Generar dataframe con nombre de juzgados y tipo de reporte,ya
#P3:Si tiene https => no agregar , si no agregar prefijo de ramajudicial,ya
#P4: Falta agregarle al archivo de .csv el timestamp de hora ejecutado
#P5: Enviar correo con Archivo adjunto,falta OR in GoogleDrive Output
#P6: Parse PDF to check if DEMANDANTE o DEMANDADO is mentioned
#P7: Organize project with github repo and use branches (master, dev, production)

#-Funcion para detectar documentos de SharePoint, proxima semana
#-Pipeline para obtener metadata de archivo PDF(tiporeporte-juzgado-dia), proxima semana

df_act_juzgados, df_act_subjuzgados = f0_load_sheets_from_source(URL,'00ActiveOrganismoJudicial','01ActiveJuzgados')

user_name = "oficina_AVT"
#date_execution_report = 
#f4_generating_merge_subjuzgados_report(df_final_urlsubjuzgados,user_name,base_dir_gdrive+data_dir+"output_report_by_client/")
#@cami este tampoco me funciono a mi asi q use el de abajo (DMV)

f4_generating_merge_subjuzgados_report(df_final_urlsubjuzgados,user_name,"/content/drive/MyDrive/00-Scraping_RamaJudicial/project/data/output_report_by_client/")

print(base_dir_drive+data_dir+"output_report_by_client/")

