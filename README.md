# Camara de Resfriados - Analise de Ciclos

Aplicacao Streamlit para analisar ciclos de carregamento e resfriamento de carcacas suinas a partir de dados exportados do supervisório Carel Boss.

O objetivo do sistema e transformar o historico de sensores da camara em uma leitura operacional simples: identificar ciclos, calcular indicadores termicos, comparar desempenhos e gerar relatorios em PDF/Excel para acompanhamento do processo.

## Sumario

- [O que o app faz](#o-que-o-app-faz)
- [Arquitetura do projeto](#arquitetura-do-projeto)
- [Fluxo de uso](#fluxo-de-uso)
- [Indicadores principais](#indicadores-principais)
- [Regras de ciclo](#regras-de-ciclo)
- [Arquivos e pastas](#arquivos-e-pastas)
- [Rodando localmente](#rodando-localmente)
- [Configurando credenciais](#configurando-credenciais)
- [Deploy no Streamlit Cloud](#deploy-no-streamlit-cloud)
- [Seguranca e privacidade](#seguranca-e-privacidade)
- [Troubleshooting](#troubleshooting)
- [Checklist antes de publicar](#checklist-antes-de-publicar)

## O que o app faz

O sistema centraliza tres tarefas:

1. Coletar dados do supervisório Carel Boss.
2. Detectar e analisar ciclos de carregamento/resfriamento.
3. Gerar relatorios e comparativos para tomada de decisao operacional.

Na interface Streamlit, o usuario consegue:

- verificar o status do banco `data/historico.csv`;
- buscar dados novos por intervalo de datas;
- gerar relatorios em PDF para um ciclo selecionado;
- gerar relatorios em lote pelo painel de operacao, quando necessario;
- visualizar indicadores de um ciclo especifico;
- comparar ciclos entre si;
- exportar um Excel comparativo gerencial.

## Arquitetura do projeto

```text
.
├── app.py
├── gerar_relatorio.py
├── gerar_relatorio_camcarcacas.py
├── requirements.txt
├── packages.txt
├── .env.example
├── .gitignore
├── .streamlit/
│   └── secrets.toml.example
└── assets/
    ├── thoms.jpg
    └── plotter_racks.png
```

### `app.py`

Arquivo principal da aplicacao Streamlit.

Responsabilidades:

- renderizar a interface;
- carregar dados;
- detectar ciclos;
- calcular indicadores;
- mostrar graficos e tabelas;
- acionar coleta e geracao de relatorios.

### `gerar_relatorio.py`

Modulo de geracao de relatorios.

Responsabilidades:

- ler o banco consolidado;
- reaproveitar as regras de ciclo do app;
- gerar arquivos PDF e Excel;
- gerar comparativos entre ciclos;
- manter os calculos dos relatorios alinhados com os calculos da interface.

### `gerar_relatorio_camcarcacas.py`

Modulo de coleta e consolidacao de dados vindos do supervisório.

Responsabilidades:

- autenticar no Carel Boss via Selenium;
- solicitar relatorios por data;
- baixar CSVs;
- mesclar dados no banco unico `data/historico.csv`;
- deduplicar registros por timestamp canonico;
- validar falhas parciais de coleta.

### `requirements.txt`

Dependencias Python usadas pelo app e pelos relatorios.

### `packages.txt`

Dependencias de sistema para o Streamlit Cloud.

Atualmente inclui:

```text
chromium
chromium-driver
```

Isso e necessario para a coleta via Selenium.

## Fluxo de uso

### 1. Operacao

No menu lateral, escolha a secao **Operacao**.

Essa area permite:

- consultar o status do banco;
- buscar dados do supervisório;
- gerar relatorios por intervalo;
- fazer operacoes raras de manutencao.

Se faltarem dias no banco para o intervalo solicitado, o app informa e pergunta se deve buscar os dados antes de gerar os relatorios.

Se a coleta terminar com falhas parciais, os relatorios nao sao gerados automaticamente. Isso evita que o usuario receba um relatorio aparentemente valido com dados incompletos.

### 2. Ciclo individual

No menu lateral, escolha **Ciclo individual**.

Paineis disponiveis:

- **Visao geral**: indicadores principais, exportacao de PDF do ciclo selecionado, resumo por fase e graficos de comportamento termico.
- **Desempenho termico**: graficos tecnicos de DT, erro do glicol e taxas de queda.

#### Exportar PDF de um ciclo

No painel **Ciclo individual > Visao geral**, existe a secao **Exportar PDF do ciclo selecionado**.

Esse fluxo foi desenhado para ser simples e seguro:

- exporta apenas o ciclo que esta selecionado no seletor de ciclos;
- nao permite exportar varios ciclos de uma vez por engano;
- permite informar a pasta de saida do PDF;
- aceita caminho absoluto local ou caminho relativo, como `reports`;
- no Streamlit Cloud, alem de salvar no servidor, mostra um botao de download;
- bloqueia a exportacao se faltar qualquer data entre o inicio e o fim do ciclo.

Exemplo:

- ciclo inicia em 16/04;
- ciclo termina em 17/04;
- para exportar o PDF, o banco precisa ter amostras de 16/04 e 17/04.

Se alguma dessas datas nao existir no dataframe carregado, o botao de exportacao fica bloqueado e o app mostra quais datas precisam ser buscadas antes.

### 3. Comparar ciclos

No menu lateral, escolha **Comparar ciclos**.

Paineis disponiveis:

- **Gerencial (ate 4 ciclos)**: comparacao lado a lado para poucos ciclos.
- **Comparacao geral**: visao consolidada de todos os ciclos.
- **Normalizado por severidade**: comparacao corrigida por condicoes iniciais.

## Indicadores principais

O sistema calcula, entre outros:

- tempo ate o espeto atingir 7 °C;
- folga ou atraso em relacao ao limite de 16 horas;
- temperatura inicial e final do espeto;
- queda de temperatura do espeto;
- temperatura de retorno no inicio do resfriamento;
- temperatura de retorno no momento em que o espeto atinge 7 °C;
- queda do retorno ate a meta de 7 °C no espeto;
- DT medio;
- erro medio absoluto do glicol;
- percentual de tempo do glicol dentro da faixa configurada;
- umidade media;
- ventilacao media;
- taxas medias de queda do espeto e retorno.

### Queda de retorno ate 7 °C

Este indicador mede a queda da temperatura de retorno desde o inicio do resfriamento ate o momento em que o espeto atinge 7 °C.

Depois que o espeto chega ao alvo, o processo entra em uma logica de conservacao. Por isso, o indicador de queda do retorno nao deve usar o fim absoluto do ciclo, pois nessa etapa o objetivo passa a ser manter a temperatura proxima do alvo com DT menor.

## Regras de ciclo

O ciclo e detectado principalmente pela transicao do sinal de carregamento.

Regras importantes:

- um novo ciclo comeca quando o carregamento passa de `OFF` para `ON`;
- o ciclo e separado quando ha lacunas grandes de amostragem;
- o ciclo considera as fases de carregamento, resfriamento e pos-meta;
- para a leitura do espeto no final do ciclo, o app evita contaminar o relatorio com uma nova carga quente quando o operador esquece de apertar o botao de carregamento.

### Regra do espeto acima de 30 °C

Quando o espeto ja atingiu a meta de 7 °C e depois aparece uma leitura acima de 30 °C, o sistema entende que pode haver uma nova carcaca quente no espeto sem o botao de carregamento ter sido acionado.

Nesse caso, o ciclo anterior e encerrado antes dessa leitura quente.

Essa regra protege o relatorio contra falsos finais de ciclo.

## Arquivos e pastas

### Arquivos versionados

Estes arquivos devem ir para o GitHub:

```text
app.py
gerar_relatorio.py
gerar_relatorio_camcarcacas.py
requirements.txt
packages.txt
.gitignore
.env.example
.streamlit/secrets.toml.example
assets/
README.md
```

### Arquivos nao versionados

Estes arquivos e pastas nao devem ir para o GitHub:

```text
.env
.venv/
data/
reports/
_reports_layout_final/
*.csv
*.pdf
*.xlsx
```

Eles podem conter dados reais, relatorios gerados, credenciais ou arquivos grandes.

O `.gitignore` ja protege esses itens quando o deploy e feito via Git.

## Rodando localmente

### 1. Criar ambiente virtual

No PowerShell, dentro da pasta do projeto:

```powershell
python -m venv .venv
```

Ative o ambiente:

```powershell
.\.venv\Scripts\Activate.ps1
```

### 2. Instalar dependencias

```powershell
pip install -r requirements.txt
```

### 3. Configurar credenciais locais

Copie o exemplo:

```powershell
Copy-Item .env.example .env
```

Edite o `.env` com os valores reais.

Importante: o `.env` nao deve ser enviado ao GitHub.

### 4. Rodar o app

```powershell
streamlit run app.py
```

## Configurando credenciais

### Local

Localmente, o app usa `.env`.

Exemplo:

```env
CAREL_HOST=seu-host-ou-ip
CAREL_PORT=8080
CAREL_USERNAME=seu_usuario
CAREL_PASSWORD=sua_senha
CAREL_REPORT_ID=158
CAREL_FREQUENCY=3
```

### Streamlit Cloud

No Streamlit Cloud, use **Secrets**.

Vá em:

```text
App > Settings > Secrets
```

Cole:

```toml
CAREL_HOST = "seu-host-ou-ip"
CAREL_PORT = "8080"
CAREL_USERNAME = "seu_usuario"
CAREL_PASSWORD = "sua_senha"
CAREL_REPORT_ID = "158"
CAREL_FREQUENCY = "3"
```

Use os valores reais apenas no painel de Secrets do Streamlit Cloud.

Nunca coloque valores reais em:

- `README.md`;
- `.env.example`;
- `.streamlit/secrets.toml.example`;
- `app.py`;
- arquivos commitados no GitHub.

## Deploy no Streamlit Cloud

### 1. Publicar no GitHub

O repositório deve conter apenas os arquivos de codigo e configuracao.

Antes de publicar, confira se estes itens nao aparecem no commit:

```text
.env
.venv/
data/
reports/
_reports_layout_final/
*.csv
*.pdf
*.xlsx
```

### 2. Criar app no Streamlit Cloud

No Streamlit Cloud:

1. Clique em **Create app**.
2. Selecione o repositório.
3. Em **Main file path**, use:

```text
app.py
```

4. Configure os Secrets.
5. Faça o deploy.

### 3. Primeiro teste apos deploy

Teste recomendado:

1. Abrir o app.
2. Ver se a tela inicial carrega.
3. Verificar se o app informa corretamente a ausencia de `data/historico.csv`, caso ainda nao exista banco no ambiente.
4. Testar coleta de um unico dia.
5. Gerar relatorio para um intervalo pequeno.

## Seguranca e privacidade

Este projeto lida com:

- credenciais do supervisório;
- dados operacionais reais;
- relatorios gerados;
- historico de sensores.

Por isso:

- credenciais reais ficam apenas em `.env` local ou nos Secrets do Streamlit Cloud;
- dados reais ficam em `data/`, que e ignorado pelo Git;
- relatorios gerados ficam em `reports/` ou pastas locais ignoradas;
- arquivos `.pdf`, `.xlsx` e `.csv` sao ignorados por padrao;
- o repositório deve ser privado, salvo decisao explicita em contrario.

### Observacao sobre rede

O Streamlit Cloud so conseguira coletar dados do Carel Boss se o host configurado estiver acessivel a partir da internet/ambiente cloud.

Se o supervisório estiver disponivel apenas na rede interna da empresa ou via VPN, o app pode funcionar para analise de dados ja carregados, mas a coleta direta pelo Streamlit Cloud pode falhar por falta de acesso de rede.

## Troubleshooting

### O app abriu, mas nao encontra dados

Verifique:

- se existe `data/historico.csv`;
- se a pasta selecionada na barra lateral e `data`;
- se o arquivo tem o formato esperado do supervisório.

### A coleta falhou

Verifique:

- se os Secrets foram configurados;
- se `CAREL_HOST` e `CAREL_PORT` estao corretos;
- se usuario e senha estao corretos;
- se o Carel Boss esta acessivel a partir do ambiente onde o app esta rodando;
- se o Streamlit Cloud instalou Chromium via `packages.txt`.

### O relatorio nao foi gerado apos buscar dados

Se a coleta teve falha parcial, o app bloqueia a geracao automatica para evitar relatorio incompleto.

Verifique a lista de dias que falharam, corrija a coleta e tente novamente.

### Apareceu erro com `temp_retorno_ar`

Esse erro indica que o banco ou o CSV carregado nao trouxe a coluna de temperatura
de retorno de ar, que e obrigatoria para identificar as fases do ciclo e calcular
os indicadores do relatorio.

Resolucao:

1. Va em **Operacao > Coletar dados** e atualize o banco.
2. Se estiver usando CSV manual, exporte novamente incluindo a variavel `Temp retorno ar`.
3. Gere os relatorios de novo.

### PDF/Excel nao aparecem no Streamlit Cloud

No Streamlit Cloud, o usuario nao navega facilmente pelo filesystem do servidor.

Por isso, depois da geracao, o app mostra botoes de download para os arquivos criados.

### O botao de PDF do ciclo fica bloqueado

O PDF individual exige que todas as datas calendario entre o inicio e o fim do ciclo tenham dados carregados.

Exemplo: se um ciclo vai de 16/04 para 17/04, mas o banco so tem 17/04, o app bloqueia a exportacao para evitar um PDF incompleto.

Resolucao:

1. Va em **Operacao > Coletar dados**.
2. Busque a data faltante indicada na mensagem.
3. Volte para **Ciclo individual > Visao geral**.
4. Gere o PDF novamente.

### O ciclo esperado nao aparece ao filtrar uma data

O filtro de relatorios considera sobreposicao de intervalo.

Exemplo:

- ciclo inicia em 16/04;
- ciclo termina em 17/04;
- ao pedir relatorio de 17/04, esse ciclo ainda deve ser considerado porque sobrepoe o dia solicitado.

## Checklist antes de publicar

Antes de fazer push para o GitHub:

- [ ] `git status` nao mostra `.env`;
- [ ] `git status` nao mostra `data/`;
- [ ] `git status` nao mostra `reports/`;
- [ ] `git status` nao mostra `_reports_layout_final/`;
- [ ] `git status` nao mostra `.venv/`;
- [ ] `git status` nao mostra arquivos `.csv`, `.pdf` ou `.xlsx`;
- [ ] `requirements.txt` existe;
- [ ] `packages.txt` existe;
- [ ] `.streamlit/secrets.toml.example` existe;
- [ ] os Secrets reais foram configurados apenas no Streamlit Cloud.

## Comandos uteis

Ver arquivos que seriam commitados:

```powershell
git status --short
```

Ver arquivos ignorados:

```powershell
git status --short --ignored
```

Testar sintaxe Python:

```powershell
python -m py_compile app.py gerar_relatorio.py gerar_relatorio_camcarcacas.py
```

Rodar localmente:

```powershell
streamlit run app.py
```

## Estado atual recomendado

Para deploy no Streamlit Cloud, o projeto deve ficar enxuto:

- codigo principal;
- assets de marca;
- dependencias;
- exemplos de configuracao;
- README.

Dados reais, credenciais e relatorios gerados devem permanecer fora do repositório.
