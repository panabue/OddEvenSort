import random
from datetime import datetime, timedelta

# Relação 1-para-1 entre cidade, bairro e sensor
cidades_bairros_sensores = {
    "CAMPO_GRANDE": ("TIRADENTES", "S01"),
    "DOURADOS": ("VILA_TONANI", "S02"),
    "CORUMBA": ("DOM_BOSCO", "S03"),
    "TRES_LAGOAS": ("JARDIM_ALVORADA", "S04"),
    "BONITO": ("CENTRO", "S05"),
    "MIRANDA": ("CENTRO", "S06"),
    "AQUIDAUANA": ("TRINDADE", "S08"),
    "PONTA_PORA": ("GRANJA", "S09")
}

# Numero total de registros gerados
n = 10

# Coloque o nome desejado para o arquivo que sera gerado
with open("dados_teste.txt", "w") as f:
    base_date = datetime(2024, 1, 1)
    cidades = list(cidades_bairros_sensores.keys())

    for i in range(n):
        cidade = random.choice(cidades)
        bairro, sensor_id = cidades_bairros_sensores[cidade]
        data = (base_date + timedelta(days=random.randint(0, 365))).strftime("%Y%m%d")
        hora = f"{random.randint(0, 23):02d}{random.randint(0, 59):02d}{random.randint(0, 59):02d}"
        temp = round(random.uniform(10.0, 45.0), 1)

        f.write(f"{sensor_id} {data} {hora} {cidade} {bairro} {temp:.1f}\n")
