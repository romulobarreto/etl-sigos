-- Tabela para relatórios GENERAL
CREATE TABLE IF NOT EXISTS general_reports (
    "UC / MD" TEXT,
    "Status" TEXT,
    "Municipio" TEXT,
    "Tipo servico" TEXT,
    "data_execucao" DATE,
    "Cod" TEXT,
    "Data Afericao" DATE,
    "TOI" TEXT,
    "TOI Entregue" TEXT,
    "AR" TEXT,
    "Data AR" DATE,
    "MD encontrado" TEXT,
    "MD instalado" TEXT,
    "Tipo medicao" TEXT,
    "Equipe" TEXT,
    "Ramal Mono" TEXT,
    "Ramal Bi" TEXT,
    "Ramal Tri" TEXT,
    "Serv de Pedreiro" TEXT,
    "Parcelamento" TEXT,
    "RS Negociado" TEXT,
    "Hora" TIMESTAMP,
    "Backoffice" TEXT,
    "Data baixado" DATE,
    "Hora inicio servico" TIME,
    "Hora fim servico" TIME,
    "Cod Financiamento" TEXT,
    "Qtd parcela(s)" TEXT,
    "data_extracao" TIMESTAMP
);

-- Tabela para relatórios RETURN
CREATE TABLE IF NOT EXISTS return_reports (
    "UC / MD" TEXT,
    "data_execucao" DATE,
    "CODIGO" TEXT,
    "TOI" TEXT,
    "MD INSTALADO" TEXT,
    "EQUIPE" TEXT,
    "DATA RESOLVIDO" DATE,
    "MOTIVO" TEXT,
    "MOTIVO DETALHADO" TEXT,
    "MOTIVO DETALHADO 2" TEXT,
    "STATUS" TEXT,
    "data_extracao" TIMESTAMP
);