import os
import sqlite3
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from io import StringIO
from datetime import datetime
import matplotlib.pyplot as plt

# Caminho do ChromeDriver
chrome_driver_path = "C:/Users/crisg/PycharmProjects/Tech_Challenge02/chromedriver.exe"

# URL do site com a tabela
url = "https://sistemaswebb3-listados.b3.com.br/indexPage/day/IBOV?language=pt-br"

# Diretório base para salvar os arquivos
base_dir = "C:/Users/crisg/Desktop/tabelas/"
raw_dir = os.path.join(base_dir, "raw")
refined_dir = os.path.join(base_dir, "refined")
os.makedirs(raw_dir, exist_ok=True)
os.makedirs(refined_dir, exist_ok=True)

# Função principal do pipeline
def pipeline():
    # 1. Obter os dados brutos
    df_bruto = obter_dados_brutos()

    # 2. Processar os dados (ETL)
    df_refinado, data_atual = processar_dados(df_bruto)

    # 3. Salvar dados refinados no Parquet
    salvar_dados_refinados(df_refinado, data_atual)

    # 4. Catalogar os dados no SQLite
    catalogar_dados(df_refinado)

    # 5. Visualizar os dados
    visualizar_dados()

# Função para obter os dados brutos via Selenium
import time

def obter_dados_brutos():
    # Configurar o Selenium
    service = Service(chrome_driver_path)
    driver = webdriver.Chrome(service=service)

    try:
        # Acessar a página
        print("Acessando a página...")
        driver.get(url)
        driver.implicitly_wait(20)
        print("Página carregada. Aguardando para visualização...")
        time.sleep(5)  # Pausa para visualizar a página carregada

        # Selecionar o maior valor no dropdown
        try:
            print("Tentando localizar o dropdown...")
            select_element = WebDriverWait(driver, 40).until(
                EC.presence_of_element_located((By.ID, "selectPage"))
            )
            print("Dropdown localizado. Pausa para visualização...")
            time.sleep(5)  # Pausa para observar o dropdown

            # Encontrar todas as opções no dropdown
            options = select_element.find_elements(By.TAG_NAME, "option")
            valores = [int(option.text) for option in options]
            maior_valor = max(valores)
            print(f"Maior valor encontrado no dropdown: {maior_valor}")
            time.sleep(5)  # Pausa para observar os valores encontrados

            # Alterar o valor do dropdown para o maior valor
            print(f"Alterando o valor do dropdown para '{maior_valor}'...")
            driver.execute_script("arguments[0].value = arguments[1];", select_element, str(maior_valor))
            time.sleep(5)  # Pausa para observar o dropdown alterado

            # Verificar o valor selecionado
            valor_atual = driver.execute_script("return arguments[0].value;", select_element)
            print(f"Valor selecionado no dropdown: {valor_atual}")

            # Acionar o evento 'change' no dropdown
            print("Disparando o evento 'change' no dropdown...")
            driver.execute_script("arguments[0].dispatchEvent(new Event('change'));", select_element)
            print("Evento 'change' disparado. Aguardando o carregamento da tabela...")
            time.sleep(5)  # Pausa para observar o evento antes do carregamento da tabela

            # Aguardar o carregamento completo da tabela
            WebDriverWait(driver, 40).until(
                EC.presence_of_element_located((By.XPATH, "//table[contains(@class, 'table-responsive-md')]"))
            )
            print("Tabela carregada com sucesso. Pausa para visualização...")
            time.sleep(5)  # Pausa para visualizar a tabela carregada
        except Exception as e:
            print(f"Erro ao selecionar o maior valor no dropdown: {e}")
            driver.save_screenshot("erro_dropdown.png")
            print("Captura de tela salva: 'erro_dropdown.png'")
            raise

        # Capturar o HTML renderizado
        html = driver.page_source

        # Salvar o HTML para depuração
        with open("pagina_debug.html", "w", encoding="utf-8") as f:
            f.write(html)
        print("HTML da página salvo para depuração: 'pagina_debug.html'")

        # Localizar a tabela no HTML renderizado
        soup = BeautifulSoup(html, "html.parser")
        table = soup.find("table", {"class": "table table-responsive-sm table-responsive-md"})
        if not table:
            raise ValueError("Tabela não encontrada na página.")

        # Converter a tabela HTML em DataFrame do Pandas
        print("Convertendo a tabela em DataFrame...")
        df = pd.read_html(StringIO(str(table)))[0]
        print("Tabela extraída com sucesso.")
        return df
    finally:
        driver.quit()

# Função para processar os dados (ETL)

def processar_dados(df):
    # Obter a data atual para a partição
    data_atual = datetime.now().strftime("%Y-%m-%d")

    # Remover separadores de milhar e converter para inteiro
    df["Qtde. Teórica"] = df["Qtde. Teórica"].str.replace(r"[.,]", "", regex=True).astype(int)

    # A. Agrupamento numérico
    df_agrupado = df.groupby("Código").agg({"Qtde. Teórica": "sum"}).reset_index()

    # B. Renomear duas colunas
    df_agrupado.rename(
        columns={"Código": "Nome_da_Acao", "Qtde. Teórica": "Quantidade_Total"},
        inplace=True
    )

    # C. Cálculo com campos de data
    df_agrupado["Data_de_Processamento"] = data_atual

    # Excluir linhas indesejadas
    excluir_valores = ["Quantidade Teórica Total", "Redutor"]
    df_agrupado = df_agrupado[~df_agrupado["Nome_da_Acao"].isin(excluir_valores)]

    # Remover uma das colunas de data duplicadas (exemplo: 'data_de_processamento')
    if "Data_de_Processamento" in df_agrupado.columns and "data_referencia" in df_agrupado.columns:
        df_agrupado = df_agrupado.drop(columns=["Data_de_Processamento"])

    return df_agrupado, data_atual


# Função para salvar dados refinados no Parquet
def salvar_dados_refinados(df, data_atual):
    partition_dir = os.path.join(refined_dir, f"data_referencia={data_atual}")
    os.makedirs(partition_dir, exist_ok=True)
    output_path = os.path.join(partition_dir, "dados_refinados.parquet")
    df.to_parquet(output_path, index=False)
    print(f"Dados refinados salvos em {output_path}")

# Função para catalogar os dados no SQLite
def catalogar_dados(df):
    conn = sqlite3.connect("catalogo_glue.db")
    cursor = conn.cursor()

    # Criar tabela no banco
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dados_refinados (
            Nome_da_Acao TEXT,
            Quantidade_Total INTEGER,
            Data_de_Processamento TEXT
        )
    """)

    # Inserir dados do Parquet no SQLite
    df.to_sql("dados_refinados", conn, if_exists="replace", index=False)

    conn.commit()
    conn.close()
    print("Dados catalogados no SQLite.")

# Função para visualizar os dados
def visualizar_dados():
    conn = sqlite3.connect("catalogo_glue.db")
    query = "SELECT Nome_da_Acao, Quantidade_Total FROM dados_refinados"
    df = pd.read_sql_query(query, conn)

    # Criar gráfico de barras
    df.plot(kind="bar", x="Nome_da_Acao", y="Quantidade_Total", legend=False)
    plt.title("Quantidade Total por Ação")
    plt.xlabel("Nome_da_Acao")
    plt.ylabel("Quantidade_Total")
    plt.tight_layout()
    plt.show()

    conn.close()

# Executar o pipeline
if __name__ == "__main__":
    pipeline()
