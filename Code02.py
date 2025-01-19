import boto3
import time
import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from io import StringIO
import pandas as pd
from datetime import datetime

# Configuração AWS
aws_access_key = "SUA_ACCESS_KEY"
aws_secret_key = "SUA_SECRET_KEY"
region_name = "sua-regiao"
bucket_name = "nome-do-seu-bucket"

# Configurar o cliente S3
s3_client = boto3.client(
    "s3",
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key,
    region_name=region_name,
)

# Configurações para o Chrome no Lambda
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--single-process")
chrome_options.add_argument("--disable-gpu")
chrome_options.binary_location = "/opt/chrome/chrome"
chrome_driver_path = "/opt/chromedriver"

# URL da página da B3
url = "https://sistemaswebb3-listados.b3.com.br/indexPage/day/IBOV?language=pt-br"

def obter_dados_brutos():
    """Faz o scrap da tabela do site da B3 no Lambda."""
    print("Iniciando o Chrome...")
    driver = webdriver.Chrome(executable_path=chrome_driver_path, options=chrome_options)

    try:
        # Acessar a página
        print("Acessando a página...")
        driver.get(url)
        time.sleep(5)  # Aguardar o carregamento inicial da página

        # Selecionar o maior valor no dropdown
        try:
            print("Tentando localizar o dropdown...")
            select_element = WebDriverWait(driver, 40).until(
                EC.presence_of_element_located((By.ID, "selectPage"))
            )
            options = select_element.find_elements(By.TAG_NAME, "option")
            valores = [int(option.text) for option in options]
            maior_valor = max(valores)
            print(f"Maior valor encontrado no dropdown: {maior_valor}")

            # Alterar o valor do dropdown para o maior valor
            driver.execute_script("arguments[0].value = arguments[1];", select_element, str(maior_valor))
            driver.execute_script("arguments[0].dispatchEvent(new Event('change'));", select_element)
            time.sleep(5)  # Aguardar o carregamento da tabela

            # Aguardar o carregamento completo da tabela
            WebDriverWait(driver, 40).until(
                EC.presence_of_element_located((By.XPATH, "//table[contains(@class, 'table-responsive-md')]"))
            )
        except Exception as e:
            print(f"Erro ao selecionar o maior valor no dropdown: {e}")
            raise

        # Capturar o HTML renderizado
        html = driver.page_source

        # Localizar a tabela no HTML renderizado
        soup = BeautifulSoup(html, "html.parser")
        table = soup.find("table", {"class": "table table-responsive-sm table-responsive-md"})
        if not table:
            raise ValueError("Tabela não encontrada na página.")

        # Converter a tabela HTML em DataFrame do Pandas
        df = pd.read_html(StringIO(str(table)))[0]
        print("Tabela extraída com sucesso.")
        return df
    finally:
        driver.quit()

def salvar_no_s3(df, data_referencia):
    """Salvar DataFrame no S3 em formato parquet particionado por data."""
    file_name = f"raw/data_referencia={data_referencia}/dados_brutos.parquet"
    df.to_parquet("/tmp/dados_brutos.parquet", index=False)
    s3_client.upload_file("/tmp/dados_brutos.parquet", bucket_name, file_name)
    print(f"Dados salvos no S3: {file_name}")

def lambda_handler(event, context):
    """Função Lambda para executar o pipeline."""
    data_referencia = datetime.now().strftime("%Y-%m-%d")
    print("Executando o pipeline...")
    df = obter_dados_brutos()  # Coletar os dados
    salvar_no_s3(df, data_referencia)  # Salvar no S3
    print("Pipeline concluído.")
