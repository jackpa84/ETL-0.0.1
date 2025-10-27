                        ETL System - Sales Data Processing
📋 Visão Geral
Sistema ETL (Extract, Transform, Load) completo desenvolvido em Python 3.13 para processamento de dados de vendas com arquitetura modular em camadas.

🚀 Como Usar Instalação bash
# Criar ambiente virtual
python -m venv venv
source venv/bin/activate  # Linux/Mac

# Instalar dependências
pip install pandas python-dateutil
Execução
bash

python main.py

📊 Fluxo de Dados
Entrada
vendas.csv: Dados de vendas em formato CSV

clientes.json: Dados de clientes em formato JSON

Processamento
Extração: Leitura dos arquivos de entrada

Transformação: Validação e enriquecimento dos dados

Carga: Export para múltiplos formatos

Saída
SQLite: Banco de dados relacional

CSV: Arquivo planilha

JSON: Dados estruturados

🔧 Configuração
Edite config/settings.py para personalizar:

Caminhos de arquivos

Parâmetros de processamento

Formatos de data

🐛 Solução de Problemas
Erros Comuns
Módulo não encontrado: pip install pandas

Arquivos não encontrados: Verifique diretório data/input/

Acesso ao SQLite: sqlite3 data/output/vendas_processadas.db

📝 Licença
MIT License