Lead Data Hygiene & ETL Pipeline

Descrição
Pipeline de Engenharia de Dados focado em Qualidade de Dados (Data Quality) para CRM. O script ingere listas de contatos despadronizadas e realiza a higienização automática dos números de telefone (adicionando DDI 55, removendo máscaras) e validação de e-mails.

Aplicação Prática
Essencial para empresas de Disparo de Mensagens (SMS/Voz), onde números incorretos geram custos desnecessários e baixam a taxa de entregabilidade.

Funcionalidades

Regex para limpeza de caracteres especiais.

Padronização de formato 55+DDD+Numero.

Flag automática de leads qualificados vs. inválidos.

Tecnologias

Python

Regular Expressions (Regex)

Pandas