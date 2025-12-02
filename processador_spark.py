import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, to_date, date_format, sum, count, max, min, split as spark_split

# 1. Configuração da Sessão
spark = SparkSession.builder \
    .appName("DashboardsFinanceirosCompleto") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# 2. Leitura do Socket
lines = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .option("maxFilesPerTrigger", 1) \
    .load()

# 3. Tratamento (ETL)
dados = split(lines.value, ";")

# Mapeamento das colunas (0:ID, 1:Valor, 2:Data, 3:Hora, 4:Inst, 5:Tipo)
df_transacoes = lines \
    .withColumn("id", dados.getItem(0)) \
    .withColumn("valor", dados.getItem(1).cast("double")) \
    .withColumn("data_str", dados.getItem(2)) \
    .withColumn("hora_completa", dados.getItem(3)) \
    .withColumn("instituicao", dados.getItem(4)) \
    .withColumn("tipo", dados.getItem(5))

# Formatação e Extração da Hora (Ex: "17:30" -> "17")
df_formatado = df_transacoes \
    .withColumn("data", to_date(col("data_str"), "dd/MM/yyyy")) \
    .withColumn("mes_ano", date_format(col("data"), "yyyy-MM")) \
    .withColumn("hora_simples", spark_split(col("hora_completa"), ":").getItem(0).cast("int")) \
    .filter(col("valor").isNotNull() & col("data").isNotNull())

# 4. TABELA MESTRE (Agregação Granular)
# Agrupamos por TUDO (Inclusive Hora) para poder derivar as estatísticas depois.
# Calculamos Max e Min aqui para o Pandas encontrar o Max/Min global depois.
df_mestre = df_formatado.groupBy("mes_ano", "instituicao", "tipo", "hora_simples") \
    .agg(
        sum("valor").alias("soma_valor"),
        count("id").alias("qtd_transacoes"),
        max("valor").alias("max_valor"),
        min("valor").alias("min_valor")
    )

# 5. Função de Processamento (ForeachBatch)
def processar_metricas(df, batch_id):
    pdf = df.toPandas()
    
    if not pdf.empty:
        # --- ARQUIVO 1: KPI GERAIS (Resumão) ---
        # Aqui calculamos os totais globais baseados nos subtotais
        total_proc = pdf['soma_valor'].sum()
        qtd_total = pdf['qtd_transacoes'].sum()
        ticket_medio = total_proc / qtd_total if qtd_total > 0 else 0

        count = df.count()
        # B. Printa a informação
        print(f"--- Batch ID: {batch_id} ---")
        print(f"Quantidade de linhas processadas: {count}")
        
        # Maior e Menor transação global
        maior_t = pdf['max_valor'].max()
        menor_t = pdf['min_valor'].min()
        
        # Hora de maior movimento (Soma as qtds por hora e pega a maior)
        hora_pico = pdf.groupby('hora_simples')['qtd_transacoes'].sum().idxmax()
        
        # Tipo mais comum
        tipo_comum = pdf.groupby('tipo')['qtd_transacoes'].sum().idxmax()
        
        df_kpi = pd.DataFrame([{
            "total_processado": total_proc,
            "qtd_total": qtd_total,
            "media_geral": ticket_medio,
            "maior_transacao": maior_t,
            "menor_transacao": menor_t,
            "hora_pico": int(hora_pico),
            "tipo_mais_comum": tipo_comum
        }])
        df_kpi.to_csv("dash_kpis_gerais.csv", index=False)
        
        # --- ARQUIVO 2: Análise por Mês ---
        df_mes = pdf.groupby('mes_ano').agg(
            total_mes=('soma_valor', 'sum'),
            qtd_mes=('qtd_transacoes', 'sum')
        ).reset_index()
        # Calcula média do mês
        df_mes['ticket_medio_mes'] = df_mes['total_mes'] / df_mes['qtd_mes']
        df_mes.to_csv("dash_analise_mes.csv", index=False)

        # --- ARQUIVO 3: Análise por Instituição ---
        df_inst = pdf.groupby('instituicao')[['soma_valor']].sum().reset_index()
        df_inst = df_inst.sort_values('soma_valor', ascending=False)
        df_inst.to_csv("dash_analise_inst.csv", index=False)

        # --- ARQUIVO 4: Distribuição Horária (Extra) ---
        # Para fazer um gráfico de linha mostrando movimento por hora do dia
        df_hora = pdf.groupby('hora_simples')[['qtd_transacoes', 'soma_valor']].sum().reset_index()
        df_hora = df_hora.sort_values('hora_simples')
        df_hora.to_csv("dash_analise_hora.csv", index=False)
        
        print(f"Batch {batch_id}: KPIs, Mês, Instituição e Horários atualizados!")

# 6. Iniciar Stream
query = df_mestre.writeStream \
    .trigger(processingTime="5 seconds") \
    .outputMode("complete") \
    .foreachBatch(processar_metricas) \
    .start()

print(">>> Sistema de Análise Iniciado.")
print(">>> Gerando: dash_kpis_gerais.csv, dash_analise_mes.csv, dash_analise_inst.csv, dash_analise_hora.csv")
query.awaitTermination()