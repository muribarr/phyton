#!/usr/bin/env python
# coding: utf-8

# ## Notebook 1
# 
# New notebook

# In[1]:


# Instalando biblioteca yfinance
# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %pip install yfinance


# In[2]:


from azure.storage.blob import BlobServiceClient

# Configurações
STORAGE_ACCOUNT_NAME = "caprojetofinalmj"
CONTAINER_NAME = "landingzone"
STORAGE_KEY = "qpNGmdyk3Pe2OOCoLAB+rwWC1gE/8i1YP+fHloKuVPZ1Ni++pDo6Pj4nNXlmMJRkPCv+Vp1qcLYf+AStWVYtjw=="  # Ou use um token SAS

# Conectar ao serviço Blob
connection_string = f"DefaultEndpointsProtocol=https;AccountName={STORAGE_ACCOUNT_NAME};AccountKey={STORAGE_KEY};EndpointSuffix=core.windows.net"
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# Acessar o container
container_client = blob_service_client.get_container_client(CONTAINER_NAME)

# Listar blobs no container
print("Blobs disponíveis:")
for blob in container_client.list_blobs():
    print(blob.name)

# Gravar um arquivo no container
blob_name = "exemplo.csv"
data = b"col1,col2,col3\n1,2,3\n4,5,6"
blob_client = container_client.get_blob_client(blob_name)
blob_client.upload_blob(data, overwrite=True)
print(f"Arquivo {blob_name} enviado com sucesso!")


# In[3]:


# Pacotes de Aquisição e Manipulação de Dados
import yfinance as yf
import pandas as pd
from datetime import datetime


# Definindo o ativo e o período de extração
ticker = "AAPL"  # Código da Apple na bolsa
start_date = "2022-01-01"
end_date = datetime.today().strftime('%Y-%m-%d')


# Extraindo os dados
data = yf.download(ticker, start=start_date, end=end_date)



# Reseta o índice para mover a data como coluna
data.reset_index(inplace=True)  

# Mostrando os primeiros dados
display(data.head())


# In[5]:


mount_point = "wasbs://landingzone@caprojetofinalmj.blob.core.windows.net"

spark.conf.set(
    "fs.azure.account.key.caprojetofinalmj.blob.core.windows.net",
    STORAGE_KEY
)

# Caminho completo no Blob Storage
# output_path = f"{mount_point}/yahoo_finance_aapl.parquet"

# Adicionar timestamp ao caminho de saída
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
output_path = f"{mount_point}/yahoo_finance_aapl_{timestamp}.parquet"

# Salvando os dados em formato Parquet (otimizado para leitura e armazenamento)
df_spark = spark.createDataFrame(data)  # Convertendo pandas dataframe para spark dataframe
df_spark.write.format("parquet").mode("overwrite").save(output_path)

print(f"Arquivo Parquet gravado com sucesso em: {output_path}")



from azure.storage.blob import BlobServiceClient

# Conectar ao container
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
container_client = blob_service_client.get_container_client("landingzone")

# Listar arquivos no container
print("Arquivos no container:")
for blob in container_client.list_blobs():
    print(blob.name)




