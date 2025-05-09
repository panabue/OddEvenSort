import random
from datetime import datetime, timedelta

cidades = ["CAMPO_GRANDE", "DOURADOS", "CORUMBA", "TRES_LAGOAS"]
bairros = ["CENTRO", "VILA_CARLOTA", "JARDIM_PANORAMA", "COHAB"]
n = 10  # Altere para 1e5, 1e6, 1e7

with open("dados_teste.txt", "w") as f:
    base_date = datetime(2025, 1, 1)
    for i in range(n):
        sensor_id = f"S{i%1000:02d}"
        data = (base_date + timedelta(days=random.randint(0, 365))).strftime("%Y%m%d")
        hora = f"{random.randint(0, 23):02d}{random.randint(0, 59):02d}{random.randint(0, 59):02d}"
        cidade = random.choice(cidades)
        bairro = random.choice(bairros)
        temp = round(random.uniform(10.0, 45.0), 1)
        f.write(f"{sensor_id} {data} {hora} {cidade} {bairro} {temp:.1f}\n")
