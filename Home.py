import time
import pyodbc
import pandas as pd
import streamlit as st
import plotly.express as px  
import datetime
import math
import locale

locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')  # ativa formatação brasileira (se disponível)


st.markdown("""
<style>
    /* Seletor para o texto dentro dos botões das abas */
    .stTabs [data-baseweb="tab-list"] button [data-testid="stMarkdownContainer"] p {
        font-size: 1.5rem; /* Altera o tamanho da fonte */
        font-family: "Courier New", Courier, monospace; /* Altera a família da fonte */
    }
</style>
""", unsafe_allow_html=True)


st.set_page_config(layout="wide")

# # --- Cabeçalho acima do menu de páginas ---
# st.markdown(
#     """
#     <style>
#         /* Posiciona o cabeçalho acima da lista de páginas */
#         section[data-testid="stSidebar"] > div:first-child::before {
#             content: "📁  Menu Principal";
#             display: block;
#             font-weight: bold;
#             font-size: 1.2rem;
#             color: #4CAF50;
#             margin: 10px 0 5px 10px;
#         }
#     </style>
#     """,
#     unsafe_allow_html=True
# )


st.set_page_config(layout="wide")
st.title("📊 Faturamento de Clientes")
st.subheader("Central de faturamentos dos clientes SIM Consultas.")

@st.cache_resource

def get_db_connection():
    server = "db-ws.sc.prod"
    database = "sim"
    user = "scadmin"
    password = "xunil2015@"  

    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={user};"
        f"PWD={password};"
        "Encrypt=no;"
        "TrustServerCertificate=yes;"
    )

    try:
        conn = pyodbc.connect(conn_str, timeout=5)
        return conn
    except pyodbc.Error as e:
        st.error(f"❌ Erro ao conectar ao banco:\n**{e}**")
        return None


tab1, tab2, tab3, tab4 = st.tabs(["RTE |", "Alfa |", "Votorantim |", "ADM"])

def get_Movimento():
    conn = get_db_connection()
    if conn is None:
        st.warning("⚠️ Conexão com o banco não foi estabelecida.")
        return pd.DataFrame()

    start_time = time.time()
    with st.spinner("⏳ Carregando conteúdo..."):
        try:
            # 🔹 Usar """ para facilitar leitura e evitar erros de aspas
            query = """
            DECLARE @DtInicio AS Datetime;
            DECLARE @DtFim AS Datetime;

            SET @DtInicio = CAST(LEFT(CONVERT(VARCHAR, DATEADD(MONTH, -1, GETDATE()) 
                - DAY(DATEADD(MONTH, -0, GETDATE())) + 1, 121), 11) + '00:00:00' AS DATETIME);
            SET @DtFim = DATEADD(DAY, -1, CAST(LEFT(CONVERT(VARCHAR, DATEADD(MONTH, -0, GETDATE()) 
                - DAY(DATEADD(MONTH, -0, GETDATE())) + 1, 121), 11) + '23:59:59' AS DATETIME));

            SELECT 
                Tipo_Consulta,
                ChaveAcesso,
                Modalidade,
                Cod_Cli,
                Cliente,
                Cod_Sistema,
                Sistema,
                DATA,
                Mês,
                Qtde,
                Vol_Total,
                Valor_Und,
                CONVERT(decimal(12,4), Vol_Total) * Valor_Und AS Valor_Fatura
            FROM (
                SELECT 
                    Tipo_Consulta,
                    ChaveAcesso,
                    Modalidade,
                    Cod_Cli,
                    Cliente,
                    Cod_Sistema,
                    Sistema,
                    DATA,
                    Mês,
                    Qtde,
                    Vol_Total,
                    0.04 AS Valor_Und
                FROM (
                    SELECT  
                        Tipo_Consulta,
                        ChaveAcesso,
                        Cod_Modalidade,
                        Modalidade,
                        Cod_Cli,
                        Cliente,
                        Cod_Sistema,
                        Sistema,
                        DATA,
                        Mês,
                        Qtde,
                        SUM(Qtde) OVER (PARTITION BY Mês, Cod_Cli) AS Vol_Total
                    FROM (
                        -- WEBSERVICE
                        SELECT  
                            'Consulta Individual' AS Tipo_Consulta,
                            a.clientechave AS ChaveAcesso,
                            b.idcliente AS Cod_Cli,
                            b.descricao AS Cliente,
                            d.CLI_MODALI AS Cod_Modalidade,
                            e.Descricao AS Modalidade,
                            a.idSistema AS Cod_Sistema,
                            c.nome AS Sistema,
                            CAST(a.DtInicioConsulta AS DATE) AS DATA,
                            DATEPART(MONTH, a.DtInicioConsulta ) AS Mês,
                            COUNT(*) AS Qtde
                        FROM sim_movimento_consulta a WITH(NOLOCK)
                        JOIN SIM_CLIENTE_CHAVEACESSO b WITH(NOLOCK) ON A.CLIENTECHAVE = b.ChaveAcesso
                        JOIN sim_sistema c WITH(NOLOCK) ON a.idsistema = c.id
                        JOIN DBBIGH.DBO.TBH_CLIENT d WITH(NOLOCK) ON B.IdCliente = D.CLI_IDENTI
                        JOIN DBLOTE.SIM.DBO.Tb_Cli_Modalidade e WITH(NOLOCK) ON d.CLI_MODALI = e.Modalidade
                        WHERE A.DmStatus IN (SELECT Id FROM SIM_STATUS WHERE Cobra = 1)	
                            AND a.DtInicioConsulta BETWEEN @DtInicio AND @DtFim
                            AND b.idcliente IN (617)
                        GROUP BY a.clientechave, d.CLI_IDENTI, d.CLI_MODALI, e.Descricao, 
                                 b.idcliente, b.descricao, a.idSistema, c.nome, 
                                 CAST(a.DtInicioConsulta AS DATE), DATEPART(MONTH, a.DtInicioConsulta)

                        UNION ALL

                        -- LOTE DB-LOTE
                        SELECT  
                            'Lote Batch' AS Tipo_Consulta,
                            b.chaveacesso AS ChaveAcesso,
                            b.idcliente AS Cod_Cli,
                            b.descricao AS Cliente,
                            d.CLI_MODALI AS Cod_Modalidade,
                            e.Descricao AS Modalidade,
                            t.SISTEMA AS Cod_Sistema,
                            c.nome AS Sistema,
                            CAST(t.DATA AS DATE) AS DATA,
                            DATEPART(MONTH, t.DATA) AS Mês,
                            COUNT(*) AS Qtde
                        FROM (
                            SELECT 
                                A.IdAutoEnvio, A.IdAuto, A.IdEstado, C.IDCLIENTE, 
                                C.DTENTRADA DATA, A.IDAUTO SISTEMA, COUNT(A.IdAutoEnvio) QTDE
                            FROM DBLOTE.SIM.DBO.AUTO_ENVIO A WITH(NOLOCK)
                            LEFT JOIN DBLOTE.SIM.DBO.SIM_PROCESSAMENTO C WITH(NOLOCK) 
                                ON A.IDPROCESSAMENTO = C.ID
                            WHERE C.DTENTRADA BETWEEN @DtInicio AND @DtFim
                            GROUP BY A.IdAutoEnvio, A.IdAuto, A.IdEstado, C.IDCLIENTE, C.DTENTRADA, A.IDAUTO
                        ) T
                        JOIN SIM_CLIENTE_CHAVEACESSO b WITH(NOLOCK) ON T.IdCliente = b.IdCliente
                        JOIN SIM_SISTEMA c WITH(NOLOCK) ON t.SISTEMA = c.id
                        JOIN DBBIGH.DBO.TBH_CLIENT d WITH(NOLOCK) ON B.IdCliente = D.CLI_IDENTI
                        JOIN DBLOTE.SIM.DBO.Tb_Cli_Modalidade e WITH(NOLOCK) ON d.CLI_MODALI = e.Modalidade
                        WHERE t.DATA BETWEEN @DtInicio AND @DtFim AND b.idcliente IN (617)
                        GROUP BY b.chaveacesso, d.CLI_IDENTI, d.CLI_MODALI, e.Descricao, 
                                 b.idcliente, b.descricao, t.SISTEMA, c.nome, 
                                 CAST(t.DATA AS DATE), DATEPART(MONTH, t.DATA)
                    ) X
                    GROUP BY Tipo_Consulta, ChaveAcesso, Cod_Cli, Cliente, Cod_Modalidade,
                             Modalidade, Cod_Sistema, Sistema, DATA, Mês, Qtde
                ) Y
            ) Z
            """
            
            df = pd.read_sql(query, conn)
            end_time = time.time()
            tempo_execucao = (end_time - start_time)/60

            st.success(f"✅ Dados carregados com sucesso em {tempo_execucao:.2f} minutos.")
            return df

        except Exception as e:
            st.error(f"❌ Erro ao consultar base: {e}")
            return pd.DataFrame()

def get_Detalhes():
    conn = get_db_connection()
    if conn is None:
        st.warning("⚠️ Conexão com o banco não foi estabelecida.")
        return pd.DataFrame()

    start_time = time.time()
    with st.spinner("⏳ Carregando conteúdo..."):
        try:
            # 🔹 Usar """ para facilitar leitura e evitar erros de aspas
            query = """
                DECLARE @DtInicio as Datetime
                DECLARE @DtFim as Datetime

                --SET @DtInicio = CAST(LEFT(CONVERT(VARCHAR, DATEADD(MONTH, -1, GETDATE()) - DAY(DATEADD(MONTH, -0, GETDATE())) + 1, 121), 11) + '00:00:00' AS DATETIME)
                SET @DtInicio = '2022-11-22 19:00:00'
                SET @DtFim = DATEADD(DAY, -1, CAST(LEFT(CONVERT(VARCHAR, DATEADD(MONTH, -0, GETDATE()) - DAY(DATEADD(MONTH, -0, GETDATE())) + 1, 121), 11) + '23:59:59' AS DATETIME))

                SELECT  a.Id as 'CodigoConsultaSIM',
                    FORMAT(a.DtInicioConsulta, 'MM/yyyy') AS Mes,
                    c.nome as 'Descrição Sistema',
                CASE
                    WHEN c.id in (17, 182) THEN d.STATUSNFE
                    ELSE d.Status
                END Status,
                '' as DOCUMENTO,
                a.cnpj as DOC,
                d.Cobra as 'Sucesso',
                convert(varchar(10),a.[DtInicioConsulta],103) DATA,
                a.DtInicioConsulta dt,
                convert(varchar(10),a.DtInicioConsulta,103) dt_fim,
                A.UF,
                SUBSTRING(A.IP,1,CHARINDEX('|',A.IP,1)-1) IP
            FROM sim_movimento_consulta a, 
                SIM_CLIENTE_CHAVEACESSO b,
                sim_sistema c,
                SIM_STATUS d
            WHERE a.CLIENTECHAVE = b.ChaveAcesso
                and a.idsistema = c.id
                and a.dmstatus = d.id
                and b.idcliente = 617
                and a.DtInicioConsulta >= @DtInicio
                and a.DtInicioConsulta <= @DtFim
                and A.DmStatus in (select Id from SIM_STATUS where Cobra = 1)
            """            
            
            df = pd.read_sql(query, conn)
            end_time = time.time()
            tempo_execucao = (end_time - start_time)/60

            #st.success(f"✅ Dados carregados com sucesso em {tempo_execucao:.2f} minutos.")
            return df

        except Exception as e:
            st.error(f"❌ Erro ao consultar base: {e}")
            return pd.DataFrame()

df_Detalhe = get_Detalhes()

st.divider()

with tab1:
    df_Fat_RTE = get_Movimento()
    st.divider()
    st.header("📋 Visão Geral do Faturamento - RTE")
    st.header("Mês/Ano referência: " f"{df_Fat_RTE['Mês'].max()}/{datetime.datetime.now().year}")

    st.divider()
    page_size = 500
    num_pages = math.ceil(len(df_Detalhe) / page_size)

    ultimo_mes_detalhe = df_Detalhe["Mes"].max()
    df_mes_detalhe = df_Detalhe[df_Detalhe["Mes"] == ultimo_mes_detalhe].copy()

    lenRTE = f"{len(df_Fat_RTE):,.0f}"
    lenRTE_Format = lenRTE.replace(',','.')

    lenDetalhes = f"{len(df_mes_detalhe):,.0f}"
    lenDetalhes_Format = lenDetalhes.replace(',','.')

    today = datetime.datetime.today()

    ValorFatura = df_Fat_RTE['Valor_Fatura'].max()
    ValorFatura_Format = f"R$ {ValorFatura:,.2f}".replace(",", "_").replace(".", ",").replace("_", ".")

    col1, col2, col3 = st.columns(3)
    col1.metric("Registros Total", lenDetalhes_Format)
    col2.metric("Valor Fatura", ValorFatura_Format)
    col3.metric("Última Atualização", f"{today.strftime('%d/%m/%Y %H:%M:%S')}")
    
    st.divider()
   
    col1, col2 = st.columns(2)   
    col2.header("Detalhes das Consultas - RTE") 
        # Permite baixar o arquivo completo
    @st.cache_data
    def convert_df_to_csv(df_mes_detalhe):
        return df_mes_detalhe.to_csv(index=False).encode("utf-8")

    csv = convert_df_to_csv(df_mes_detalhe)
    col2.download_button(
    label="💾 Baixar base completa em CSV",
    data=csv,
    file_name="Faturamento_RTE_Completo.csv",
    mime="text/csv",
)
        # 🔹 Define as colunas que deseja manter
    colunas_relevantes = [
    "CodigoConsultaSIM",
    "Descrição Sistema",
    "Status",
    "DOC",
    "DATA",
    "UF",
    "IP"
    ]

    # 🔹 Mantém apenas essas colunas
    colunas_existentes = [c for c in colunas_relevantes if c in df_mes_detalhe.columns]
    df_mes_detalhe = df_mes_detalhe[colunas_existentes].copy()

    # 🔹 Renomeia as colunas para exibição
    df_mes_detalhe.rename(columns={
    "CodigoConsultaSIM": "Cod. Consulta SIM",
    "Descrição Sistema": "Sistema",
    "DOC": "Parametro",
    "DATA": "Data Consulta"
    }, inplace=True)

  
    col2.dataframe(df_mes_detalhe)

    # 🔹 2. Identifica o último mês disponível
    ultimo_mes = df_Fat_RTE["Mês"].max()

    # 🔹 3. Filtra apenas o último mês
    df_mes = df_Fat_RTE[df_Fat_RTE["Mês"] == ultimo_mes].copy()

    # 🔹 4. Calcula o Valor_Fatura dinamicamente (Qtde * Valor_Und)
    df_mes["Valor_Fatura"] = df_mes["Qtde"] * df_mes["Valor_Und"]

    # 🔹 5. Agrupa e resume por sistema
    df_resumo = (
        df_mes.groupby("Sistema")[["Vol_Total", "Valor_Und", "Valor_Fatura"]]
        .sum()
        .reset_index()
        .sort_values(by="Vol_Total", ascending=False)
    )

def format_real(valor):
    """Formata número em reais, mesmo que seja float ou int"""
    if pd.isna(valor) or not isinstance(valor, (int, float)):
        return ""
    return f"R$ {valor:,.2f}".replace(",", "v").replace(".", ",").replace("v", ".")

def format_numero(valor):
    """Formata número inteiro com separador de milhar"""
    if pd.isna(valor) or not isinstance(valor, (int, float)):
        return ""
    return f"{valor:,.0f}".replace(",", "v").replace(".", ",").replace("v", ".")

    # 🔹 6. Adiciona linha de total geral (como no Power BI)
total_row = pd.DataFrame({
    "Sistema": ["Total Geral"],
    "Vol_Total": [df_resumo["Vol_Total"].sum()],
    "Valor_Und": [None],  # usa None para evitar que a coluna vire texto
    "Valor_Fatura": [df_resumo["Valor_Fatura"].sum()]
})
df_resumo = pd.concat([df_resumo, total_row], ignore_index=True)

# 🔹 7. Exibe no Streamlit formatado
col1.markdown(f"### 📊 Faturamento — Mês {ultimo_mes} - Resumo")

    # 🔹 Renomeia as colunas para exibição
df_resumo.rename(columns={
    "Vol_Total": "Vol. Total",
    "Valor_Und": "Valor Und.",
    "Valor_Fatura": "Valor Fatura"
    }, inplace=True)

# ✅ Usa lambda para checar o tipo antes de aplicar formatação
col1.dataframe(
    df_resumo.style.format({
        "Vol. Total": format_numero,
        "Valor Und.": format_real,
        "Valor Fatura": format_real
    }).set_properties(
        **{
            "border-color": "black",
            "border-width": "1px",
            "border-style": "solid",
            "text-align": "center",
            "font-weight": "bold"
        }
    ).apply(
        lambda x: [
            "font-weight: bold; " if x["Sistema"] == "Total Geral" else ""
            for _ in x
        ],
        axis=1
    )
)
with tab2:
    df_Fat_RTE = get_Movimento()
    st.divider()
    st.header("📋 Visão Geral do Faturamento - Alfa")
    st.header("Mês/Ano referência: " f"{df_Fat_RTE['Mês'].max()}/{datetime.datetime.now().year}")

    st.divider()
    page_size = 500
    num_pages = math.ceil(len(df_Detalhe) / page_size)

    ultimo_mes_detalhe = df_Detalhe["Mes"].max()
    df_mes_detalhe = df_Detalhe[df_Detalhe["Mes"] == ultimo_mes_detalhe].copy()

    lenRTE = f"{len(df_Fat_RTE):,.0f}"
    lenRTE_Format = lenRTE.replace(',','.')

    lenDetalhes = f"{len(df_mes_detalhe):,.0f}"
    lenDetalhes_Format = lenDetalhes.replace(',','.')

    today = datetime.datetime.today()

    ValorFatura = df_Fat_RTE['Valor_Fatura'].max()
    ValorFatura_Format = f"R$ {ValorFatura:,.2f}".replace(",", "_").replace(".", ",").replace("_", ".")

    col1, col2, col3 = st.columns(3)
    col1.metric("Registros Total", lenDetalhes_Format)
    col2.metric("Valor Fatura", ValorFatura_Format)
    col3.metric("Última Atualização", f"{today.strftime('%d/%m/%Y %H:%M:%S')}")
    
    st.divider()
   
    col1, col2 = st.columns(2)   
    col2.header("Detalhes das Consultas - Alfa") 
        # Permite baixar o arquivo completo
    @st.cache_data
    def convert_df_to_csv(df_mes_detalhe):
        return df_mes_detalhe.to_csv(index=False).encode("utf-8")

    csv = convert_df_to_csv(df_mes_detalhe)
    col2.download_button(
    label="💾 Baixar base completa em CSV",
    data=csv,
    file_name="Faturamento_Alfa_Completo.csv",
    mime="text/csv",
)
        # 🔹 Define as colunas que deseja manter
    colunas_relevantes = [
    "CodigoConsultaSIM",
    "Descrição Sistema",
    "Status",
    "DOC",
    "DATA",
    "UF",
    "IP"
    ]

    # 🔹 Mantém apenas essas colunas
    colunas_existentes = [c for c in colunas_relevantes if c in df_mes_detalhe.columns]
    df_mes_detalhe = df_mes_detalhe[colunas_existentes].copy()

    # 🔹 Renomeia as colunas para exibição
    df_mes_detalhe.rename(columns={
    "CodigoConsultaSIM": "Cod. Consulta SIM",
    "Descrição Sistema": "Sistema",
    "DOC": "Parametro",
    "DATA": "Data Consulta"
    }, inplace=True)

  
    col2.dataframe(df_mes_detalhe)

    # 🔹 2. Identifica o último mês disponível
    ultimo_mes = df_Fat_RTE["Mês"].max()

    # 🔹 3. Filtra apenas o último mês
    df_mes = df_Fat_RTE[df_Fat_RTE["Mês"] == ultimo_mes].copy()

    # 🔹 4. Calcula o Valor_Fatura dinamicamente (Qtde * Valor_Und)
    df_mes["Valor_Fatura"] = df_mes["Qtde"] * df_mes["Valor_Und"]

    # 🔹 5. Agrupa e resume por sistema
    df_resumo = (
        df_mes.groupby("Sistema")[["Vol_Total", "Valor_Und", "Valor_Fatura"]]
        .sum()
        .reset_index()
        .sort_values(by="Vol_Total", ascending=False)
    )

def format_real(valor):
    """Formata número em reais, mesmo que seja float ou int"""
    if pd.isna(valor) or not isinstance(valor, (int, float)):
        return ""
    return f"R$ {valor:,.2f}".replace(",", "v").replace(".", ",").replace("v", ".")

def format_numero(valor):
    """Formata número inteiro com separador de milhar"""
    if pd.isna(valor) or not isinstance(valor, (int, float)):
        return ""
    return f"{valor:,.0f}".replace(",", "v").replace(".", ",").replace("v", ".")

    # 🔹 6. Adiciona linha de total geral (como no Power BI)
total_row = pd.DataFrame({
    "Sistema": ["Total Geral"],
    "Vol_Total": [df_resumo["Vol_Total"].sum()],
    "Valor_Und": [None],  # usa None para evitar que a coluna vire texto
    "Valor_Fatura": [df_resumo["Valor_Fatura"].sum()]
})
df_resumo = pd.concat([df_resumo, total_row], ignore_index=True)

# 🔹 7. Exibe no Streamlit formatado
col1.markdown(f"### 📊 Faturamento — Mês {ultimo_mes} - Resumo")

    # 🔹 Renomeia as colunas para exibição
df_resumo.rename(columns={
    "Vol_Total": "Vol. Total",
    "Valor_Und": "Valor Und.",
    "Valor_Fatura": "Valor Fatura"
    }, inplace=True)

# ✅ Usa lambda para checar o tipo antes de aplicar formatação
col1.dataframe(
    df_resumo.style.format({
        "Vol. Total": format_numero,
        "Valor Und.": format_real,
        "Valor Fatura": format_real
    }).set_properties(
        **{
            "border-color": "black",
            "border-width": "1px",
            "border-style": "solid",
            "text-align": "center",
            "font-weight": "bold"
        }
    ).apply(
        lambda x: [
            "font-weight: bold; " if x["Sistema"] == "Total Geral" else ""
            for _ in x
        ],
        axis=1
    )
)
with tab3:
    st.header("Votoratim")
    st.dataframe()
with tab4:
    st.header("ADM")
    st.dataframe()
