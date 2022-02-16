# INICIANDO OS TRABALHOS 
################################################################################# 
#                   CRIAÇÃO DO BANCO DE DADOS PARA O TRABALHO                   #
#################################################################################
# ----
# Criando o banco de dados para armazenamento e manipulação dos dados utilizando o MySQL 

# Instalando os pacotes e criando conexao com o MySQL ----
pkg <- c("DBI","unix","odbc","tidyverse","dbplyr","data.table","installr","httr",
         "rvest", "stringi", "magrittr", "RSelenium", "lubridate") # Pacotes utilizados
install.packages(pkg[!pkg%in%rownames(installed.packages())], quiet = T) # Instalação dos pacotes
sapply(pkg, require, character.only=T) # Chamando os pacotes para o ambiente

con <- odbc::dbConnect(odbc::odbc(), 
                       driver = "SetYourDriver", 
                       server = "YourServer",
                       uid = "YourUserID", 
                       pwd = "YourPassword", 
                       port = "YourPort",
                       database = "YourDataBase",
                       timeout = 86400) # Criando conexão com o MySQL

#dbDisconnect(con) # Para desconectar do Banco de Dados MySQL

# 01 - Base de beneficiarios ----
UF <- c("XX","RR","AC","AP","AM","TO","RO","PI","SE","DF","AL","MA","PB","MS",
        "RN","PA","MT","CE","ES","GO","PE","BA","SC","RS","PR","RJ","MG","SP")
year <- 2015
month <- 1:12
#
options(timeout = max(86400, getOption("timeout")))
temp <- tempfile()
while(year<=year(Sys.time())){
  for(j in 1:length(month)){
    for(i in 1:length(UF)){
      if(http_error(paste0("http://ftp.dadosabertos.ans.gov.br/FTP/PDA/informacoes_consolidadas_de_beneficiarios/",year,str_pad(month[j],2,pad="0"),
                           "/ben",year,str_pad(month[j],2,pad="0"),"_",UF[i],".zip"))) {
        next
      } else {
        download.file(paste0("http://ftp.dadosabertos.ans.gov.br/FTP/PDA/informacoes_consolidadas_de_beneficiarios/",year,str_pad(month[j],2,pad="0"),
                             "/ben",year,str_pad(month[j],2,pad="0"),"_",UF[i],".zip"), destfile = temp, quiet = T)
        print(paste0("Estado de ",UF[i], " para a competência de ", year,"/",str_pad(month[j],2,pad="0")," baixado com sucesso"))
        a <- fread(cmd=paste0("unzip -p ",temp), encoding = "Latin-1", colClasses = "character")
        #bb <- unique(a[,.(NM_RAZAO_SOCIAL, NR_CNPJ, CD_OPERADORA)])
        ## ADICIONAR O CNPJ DA OPERADORA NA TABELA `TBL_CODOPS_NMOPS`
        names(a)[1] <- "ID_CMPT_MOVEL"
        a <- a[,.(ID_CMPT_MOVEL, CD_OPERADORA,SG_UF,CD_MUNICIPIO,TP_SEXO,DE_FAIXA_ETARIA,DE_FAIXA_ETARIA_REAJ,
                  CD_PLANO,TIPO_VINCULO,QT_BENEFICIARIO_ATIVO,QT_BENEFICIARIO_ADERIDO,QT_BENEFICIARIO_CANCELADO,
                  DT_CARGA)]
        #head(a,2)
        
        comp = as.numeric(paste0(year,str_pad(month[j],2,"left","0"))) # Criação da competência para corrigir os dados de faixa etária após 201509
        
        a$TP_SEXO <- factor(a$TP_SEXO) # Sexo com M = Macho e F = Fêmea
        a$DE_FAIXA_ETARIA <- factor(a$DE_FAIXA_ETARIA)
        
        if(comp < 201509){
          levels(a$DE_FAIXA_ETARIA) <- c("0","1","2","3","4","5","6"
                                         ,"7","8","9","10","11","12","13") # faixas etárias quinquênais até 59+ com 0 = Não Identificado
        } else if (comp >= 201509 & i==1){
          levels(a$DE_FAIXA_ETARIA) <- c("1","2","3","4","5","6"
                                         ,"7","8","9","10","11","12","13") # faixas etárias quinquênais até 59+ com 0 = Não Identificado
        } else if (comp >= 201509 & i>1){
          levels(a$DE_FAIXA_ETARIA) <- c("1","2","3","4","5","6"
                                         ,"7","8","9","10","11","12","13","0") # faixas etárias quinquênais até 59+ com 0 = Não Identificado
        }
        
        a$DE_FAIXA_ETARIA_REAJ <- factor(a$DE_FAIXA_ETARIA_REAJ)
        
        if(comp < 201509){
          levels(a$DE_FAIXA_ETARIA_REAJ) <- c("0","1","2","3","4",
                                              "5","6","7","8","9","10") # 10 faixas etárias da ANS onde 0 = Não Identificado
        } else if (comp >= 201509 & i==1){
          levels(a$DE_FAIXA_ETARIA_REAJ) <- c("1","2","3","4","5","6",
                                              "7","8","9","10") # 10 faixas etárias da ANS onde 0 = Não Identificado
        } else if (comp >= 201509 & i>1){
          levels(a$DE_FAIXA_ETARIA_REAJ) <- c("1","2","3","4","5","6",
                                              "7","8","9","10","0") # 10 faixas etárias da ANS onde 0 = Não Identificado
        }
        
        a$TIPO_VINCULO <- factor(a$TIPO_VINCULO)
        levels(a$TIPO_VINCULO) <- c("D","NI","T") # Mudando T = Titular, D = Dependente e NI = Não Identificado
        
        a$DT_CARGA <- as.Date.character(a$DT_CARGA, format = "%d/%m/%Y") # Formatando a data
        
        # Escrevendo os dados de forma automática caso a tabela não exista e acrescentando linhas caso exista
        if(!paste0("BENEF_",year)%in%dbListTables(con)){
          dbWriteTable(con, paste0("BENEF_",year), a)
        } else {
          dbWriteTable(con, paste0("BENEF_",year), a, append=T, header=F,row.names=F)
          print(paste0("Competencia de ",year,"/",str_pad(month[j],2,pad="0")," para a UF de ",UF[i]," incorporada com sucesso!"))
          
        }
      }
    }
    Sys.sleep(4)
  }
  year = year+1
}

unlink(temp, recursive = T, force = T); rm(temp, UF, year, month, a)

# 02 - Procedimentos Ambulatoriais e Hospitalares ----
# Para selecionar todos os estados do nordeste do Brasil é só rodar a parte comentada com #
#NE.BR <- c("SE","PI","AL","MA","PB","RN","CE","PE","BA")
NE.BR <- "RN" ; j=1
ano <- 2015
options(timeout = max(86400, getOption("timeout")))

while(ano < format(Sys.Date(), format = "%Y")){
  tempdir = tempdir(check = T); temp = tempfile(tmpdir = tempdir)
  #for(j in 1:length(NE.BR)){
  # Procedimentos Ambulatoriais ----
  url = paste0("http://ftp.dadosabertos.ans.gov.br/FTP/PDA/TISS/AMBULATORIAL/",ano,"/",NE.BR[j],"/")
  lista = url %>% read_html() %>% html_table(fill = T) %>% as.data.frame() %>% select(Name) %>% slice(-c(1,2,n()))
  
  amb.C = lista %>% filter(str_detect(Name,"CONS")) %>% extract2(1)
  amb.D = lista %>% filter(str_detect(Name,"DET")) %>% extract2(1)
  
  # Tabela AMB CONS
  for (i in 1:length(amb.C)){
    download.file(paste0(url, amb.C[i]),destfile = temp, quiet = T, timeout = 86400)
    amb.cons <- distinct(fread(cmd=paste0("unzip -p ",temp), colClasses = "character", drop = c(8,10)))
    
    # Categorizando o porte para P = PEQUENA, M = MEDIA, G = GRANDE, SB = SEM BENEFICIÁRIOS
    amb.cons$PORTE <- factor(amb.cons$PORTE)
    levels(amb.cons$PORTE) <- c("G","M","P", "SB") 
    
    # Salvando DB
    if(!paste0("PROC_AMB_CONS_",NE.BR[j])%in%dbListTables(con)){
      dbWriteTable(con, paste0("PROC_AMB_CONS_",NE.BR[j]), amb.cons)
    } else {
      dbWriteTable(con, paste0("PROC_AMB_CONS_",NE.BR[j]), amb.cons, append=T, header=F,row.names=F)
    }
    print(paste0("Tabela AMB CONS para o ano de ",ano," e UF de ",NE.BR[j]," incorporada com sucesso!"))
  }
  
  # Tabela AMB DET
  for (i in 1:length(amb.D)){
    download.file(paste0(url, amb.D[i]),destfile = temp, quiet = T, timeout = 86400)
    amb.det <- distinct(fread(cmd=paste0("unzip -p ",temp), colClasses = "character", drop = 2))
    
    # Mudando o nome de TABELA PRÓPRIA para TP (redução de espaço)
    amb.det[amb.det$CD_PROCEDIMENTO == "TABELA PRÓPRIA","CD_PROCEDIMENTO"] <- "TP" 
    
    # Salvando DB
    if(!paste0("PROC_AMB_DET_",NE.BR[j])%in%dbListTables(con)){
      dbWriteTable(con, paste0("PROC_AMB_DET_",NE.BR[j]), amb.det)
    } else {
      dbWriteTable(con, paste0("PROC_AMB_DET_",NE.BR[j]), amb.det, append=T, header=F,row.names=F)
    }
    print(paste0("Tabela AMB DET para o ano de ",ano," e UF de ",NE.BR[j]," incorporada com sucesso!"))
  }
  
  # Procedimentos Hospitalares ----
  url = paste0("http://ftp.dadosabertos.ans.gov.br/FTP/PDA/TISS/HOSPITALAR/",ano,"/",NE.BR[j],"/")
  lista = url %>% read_html() %>% html_table(fill = T) %>% as.data.frame() %>% select(Name) %>% slice(-c(1,2,n()))
  
  hosp.C = lista %>% filter(str_detect(Name,"CONS")) %>% extract2(1)
  hosp.D = lista %>% filter(str_detect(Name,"DET")) %>% extract2(1)
  
  # Tabela HOSP CONS
  for (i in 1:length(hosp.C)){
    download.file(paste0(url, hosp.C[i]),destfile = temp, quiet = T, timeout = 86400)
    hosp.cons <- distinct(fread(cmd=paste0("unzip -p ",temp), colClasses = "character", drop = c(8,10)))
    
    # Categorizando o porte para P = PEQUENA, M = MEDIA, G = GRANDE, SB = SEM BENEFICIÁRIO
    hosp.cons$PORTE_OPERADORA <- factor(hosp.cons$PORTE)
    levels(hosp.cons$PORTE_OPERADORA) <- c("G","M","P", "SB") 
    
    # Salvando DB
    if(!paste0("PROC_HOSP_CONS_",NE.BR[j])%in%dbListTables(con)){
      dbWriteTable(con, paste0("PROC_HOSP_CONS_",NE.BR[j]), hosp.cons)
    } else {
      dbWriteTable(con, paste0("PROC_HOSP_CONS_",NE.BR[j]), hosp.cons, append=T, header=F,row.names=F)
    }
    print(paste0("Tabela HOSP CONS para o ano de ",ano," e UF de ",NE.BR[j]," incorporada com sucesso!"))
  }
  
  # Tabela HOSP DET
  for (i in 1:length(hosp.D)){
    download.file(paste0(url, hosp.D[i]),destfile = temp, quiet = T, timeout = 86400)
    hosp.det <- distinct(fread(cmd=paste0("unzip -p ",temp), colClasses = "character", drop = 2))
    
    # Mudando o nome de TABELA PRÓPRIA para TP (redução de espaço)
    hosp.det[hosp.det$CD_PROCEDIMENTO == "TABELA PRÓPRIA", "CD_PROCEDIMENTO"] <- "TP" 
    
    # Salvando Db
    if(!paste0("PROC_HOSP_DET_",NE.BR[j])%in%dbListTables(con)){
      dbWriteTable(con, paste0("PROC_HOSP_DET_",NE.BR[j]), hosp.det)
    } else {
      dbWriteTable(con, paste0("PROC_HOSP_DET_",NE.BR[j]), hosp.det, append=T, header=F,row.names=F)
    }
    print(paste0("Tabela HOSP DET para o ano de ",ano," e UF de ",NE.BR[j]," incorporada com sucesso!"))
  }
  
  unlink(temp, recursive = T) ; unlink(tempdir, recursive = T)
  #}
  rm(url, amb.cons, amb.det, hosp.cons, hosp.det, amb.C, amb.D, hosp.C, hosp.D, temp, tempdir, lista)
  ano = ano + 1
  Sys.sleep(4)
}

rm(NE.BR, ano)

# 03 - Caracteristica dos planos ----
url <- "http://ftp.dadosabertos.ans.gov.br/FTP/PDA/caracteristicas_produtos_saude_suplementar/caracteristicas_produtos_saude_suplementar.csv"

carac_prod_ssp <- read.csv2(url, header = T, encoding = "latin1")

tbl_codOPS.nmOPS <- unique(carac_prod_ssp[,c("RAZAO_SOCIAL","CD_OPERADORA")])
tbl_codOPS.nmOPS$RAZAO_SOCIAL <- toupper(stri_trans_general(tbl_codOPS.nmOPS$RAZAO_SOCIAL, "Latin-ASCII"))
max(nchar(tbl_codOPS.nmOPS$RAZAO_SOCIAL))

tbl_idPLAN.nmPLAN <- unique(carac_prod_ssp[,c("ID_PLANO","CD_PLANO","NM_PLANO")])
tbl_idPLAN.nmPLAN$NM_PLANO <- toupper(stri_trans_general(tbl_idPLAN.nmPLAN$NM_PLANO, "Latin-ASCII"))
max(nchar(tbl_idPLAN.nmPLAN$NM_PLANO[!is.na(tbl_idPLAN.nmPLAN$NM_PLANO)]))

dbWriteTable(con, "TBL_CODOPS_NMOPS", tbl_codOPS.nmOPS, overwrite=T) # escrevendo a tabela com o codigo e nome das ops
dbWriteTable(con, "TBL_IDPLAN_NMPLAN", tbl_idPLAN.nmPLAN, overwrite=T) #escrevendo a tabela com id, cod e nome do plano

rm(tbl_codOPS.nmOPS, tbl_idPLAN.nmPLAN) #removendo as tabelas da memoria do R

# Manipulando a tabela de característica dos planos

carac_prod_ssp <- carac_prod_ssp %>% 
  select(!c("CD_PLANO","NM_PLANO","RAZAO_SOCIAL","CONTRATACAO","GR_SGMT_ASSISTENCIAL",
            "LG_ODONTOLOGICO","OBSTETRICIA","COBERTURA"))

carac_prod_ssp$GR_MODALIDADE <- factor(carac_prod_ssp$GR_MODALIDADE) # colocando o codigo para modalidade da ops
levels(carac_prod_ssp$GR_MODALIDADE) <- c("8","1","2","3","4","5","6","7") # 8: Adm; 1: Autogestão; 2: Coop. Médica; 3: Coop. Odontológica; 4: Filantropia; 5: Medicina de Grupo; 6: Odontologia de Grupo; 7: Seguradora

carac_prod_ssp$PORTE_OPERADORA <- factor(carac_prod_ssp$PORTE_OPERADORA) # Porte da OPS: P=PEQUENA; M=MEDIA; G=GRANDE; NI=NAO IDENT
levels(carac_prod_ssp$PORTE_OPERADORA) <- c("G","M","NI","P")

carac_prod_ssp$VIGENCIA_PLANO <- factor(carac_prod_ssp$VIGENCIA_PLANO) # Vingencia do Plano: A=Anterior 9656/98; P=Posterior 9656/98

carac_prod_ssp$GR_CONTRATACAO <- factor(carac_prod_ssp$GR_CONTRATACAO) # Mudando Ind/Fam = 1; Col Emp. = 2, col ad. = 3 e NI = 0
levels(carac_prod_ssp$GR_CONTRATACAO) <- c("2","3","1","0")

carac_prod_ssp$SGMT_ASSISTENCIAL <- factor(carac_prod_ssp$SGMT_ASSISTENCIAL) # Mudando Segmentacao assistencial
levels(carac_prod_ssp$SGMT_ASSISTENCIAL) <- c("13", "1","2","3","4","5","6","13","13","7","8","9","10","11","12")

carac_prod_ssp$TIPO_FINANCIAMENTO <- factor(carac_prod_ssp$TIPO_FINANCIAMENTO) # Alterando tipo de financiamento
levels(carac_prod_ssp$TIPO_FINANCIAMENTO) <- c("3","0","1","2") # 1-pos; 2-pre; 3-misto; 0-nao identificado

carac_prod_ssp$ABRANGENCIA_COBERTURA <- factor(carac_prod_ssp$ABRANGENCIA_COBERTURA) # codificando a cobertura
levels(carac_prod_ssp$ABRANGENCIA_COBERTURA) <- c("3","4","2","1","5","6") # 1-munic; 2-gr munic; 3-estad; 4-gr estad; 5-nac; 6-outras

carac_prod_ssp$FATOR_MODERADOR <- factor(carac_prod_ssp$FATOR_MODERADOR) # Mudando o fator moderador
levels(carac_prod_ssp$FATOR_MODERADOR) <- c("1","2","3","4") # 1-Ausente; 2-Copart; 3-Franq; 4-Franq+Copart

carac_prod_ssp$ACOMODACAO_HOSPITALAR <- factor(carac_prod_ssp$ACOMODACAO_HOSPITALAR) # Mudando a acomodacao hospitalar
levels(carac_prod_ssp$ACOMODACAO_HOSPITALAR) <- c("1","2","0","3") # 1-Col; 2-Ind; 3-Nao se Aplica; 0-Nao Ident

carac_prod_ssp$LIVRE_ESCOLHA <- factor(carac_prod_ssp$LIVRE_ESCOLHA) # Mudando a acomodacao hospitalar
levels(carac_prod_ssp$LIVRE_ESCOLHA) <- c("1","3","4","2") # 1-Ausente; 2-Total; 3-Parc c/ int; 4-Parc s/ int

carac_prod_ssp$SITUACAO_PLANO <- factor(carac_prod_ssp$SITUACAO_PLANO) # Mudando a acomodacao hospitalar
levels(carac_prod_ssp$SITUACAO_PLANO) <- c("A","C","S","T") # A-Ativo; C-Cancelado; S-Suspenso; T-Transferido

# Formatando as datas
carac_prod_ssp$DT_SITUACAO <- as.Date.character(carac_prod_ssp$DT_SITUACAO, format = "%d/%m/%Y")
carac_prod_ssp$DT_REGISTRO_PLANO <- as.Date.character(carac_prod_ssp$DT_REGISTRO_PLANO, format = "%d/%m/%Y")
carac_prod_ssp$DT_ATUALIZACAO <- as.Date.character(carac_prod_ssp$DT_ATUALIZACAO, format = "%d/%m/%Y")
head(carac_prod_ssp,10)

## Salvando a tabela no MySQL
dbWriteTable(con, "CARAC_PROD_SSP", carac_prod_ssp, overwrite=T)

rm(carac_prod_ssp, url)

# 04 - Valor Comercial das Mensalidades ----
url = "http://ftp.dadosabertos.ans.gov.br/FTP/PDA/nota_tecnica_ntrp_vcm_faixa_etaria/nota_tecnica_vcm_faixa_etaria.zip"

tempd = tempdir(); temp = tempfile(tmpdir = tempd)
download.file(url, destfile = temp, quiet = T)

vcm.FxEt <- fread(cmd=paste0("unzip -p ",temp), encoding = "Latin-1", colClasses = "character", sep=";", drop=c(1,5,10))
vcm.FxEt <- vcm.FxEt[vcm.FxEt$ID_PLANO!="ID_PLANO"]
vcm.FxEt$FAIXA_ETARIA <- factor(vcm.FxEt$FAIXA_ETARIA)
levels(vcm.FxEt$FAIXA_ETARIA) <- c("1","2","3","4","5","6","7","8","9","10") # 10 faixas etárias da ANS

dbWriteTable(con, "VCM_POR_FX_ETARIA", vcm.FxEt, overwrite = T)

unlink(temp, recursive = T) ; unlink(tempd, recursive = T)
rm(temp, tempd, vcm.FxEt)

# 05 - Demonstrações Contábeis das OPS (DIOPS) ----
ano <- 2015:2021
tempd <- tempdir(check = T); temp <- tempfile(tmpdir = tempd)

for(j in 1:length(ano)){
  for(i in 1:4){
    if(http_error(paste0("http://ftp.dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/",ano[j],"/",i,"T",ano[j],".zip"))){
      next
    } else {
      download.file(paste0("http://ftp.dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/",ano[j],"/",i,"T",ano[j],".zip"),
                    destfile = temp, quiet = T)
      print(paste0("Dados do DIOPS para o ", i,"º Trimestre de ",ano[j]," baixado com sucesso!"))
      a <- fread(cmd=paste0("unzip -p ",temp), encoding = "Latin-1", dec=",", colClasses = list(character=1:4,numeric=5))
    }
    a[,1] <- paste0(ano[j],str_pad(i,2,pad="0"))
    b <- a[,3:4] %>% unique()
    a <- a[,-4]
    
    if(!"DIOPS"%in%dbListTables(con)){
      dbWriteTable(con, "DIOPS", a)
      print("Tabela do DIOPS criada com sucesso!")
    } else {
      dbWriteTable(con, "DIOPS", a, append=T, header=F,row.names=F)
      print(paste0("Tabela do DIOPS do ",i,"º Trimestre de ",ano[j]," incorporado com sucesso!"))
    }
  }
}

dbWriteTable(con, "CONTAS_CONTABEIS", b)

unlink(temp, recursive = T) ; unlink(tempd, recursive = T)
rm(temp, tempd, a, b, ano)

# 06 - Reajuste dos Planos Coletivos (RPC) ----

url <- "http://ftp.dadosabertos.ans.gov.br/FTP/PDA/RPC/"
data <- url %>% read_html() %>% html_table(fill=T) %>% as.data.frame() %>% select("Name")
data <- data[-c(1:2,nrow(data)-1,nrow(data)),]

tempd=tempdir(check = T) ; temp=tempfile(tmpdir = tempd)

for (i in 1:length(data)){
  download.file(paste0("http://ftp.dadosabertos.ans.gov.br/FTP/PDA/RPC/",data[i]), destfile = temp, quiet = T)
  a <- fread(temp, encoding = "Latin-1", dec=",", colClasses = list(character=c(1:7,11:19),numeric=8:10), drop = c(2:4,15,16))
  a[,7] <- a[,7]/100
  names(a)[7] <- "VL_REAJUSTE"
  a[,"COMPETENCIA"] <- substr(data[i],nchar(data[i])-9,nchar(data[i])-4)
  
  if(!"RPC"%in%dbListTables(con)){
    dbWriteTable(con, "RPC", a)
    print(paste0("Tabela do RPC com a competencia de ", substr(data[i],nchar(data[i])-9,nchar(data[i])-4)," criada com sucesso!"))
  } else {
    dbWriteTable(con, "RPC", a, append=T, header=F,row.names=F)
    print(paste0("Tabela do RPC da competencia de ",substr(data[i],nchar(data[i])-9,nchar(data[i])-4)," incorporado com sucesso!"))
  }
}

unlink(temp, recursive = T) ; unlink(tempd, recursive = T)
remove(temp, tempd, a, data, url)

# 07 - Reajuste dos Planos Individuais (RPI) ----
url <- "https://www.gov.br/ans/pt-br/arquivos/assuntos/consumidor/reajustes-de-mensalidade/reajuste-anual-de-planos-individuais-familiares/metodologia-de-calculo/tabela_reajustes_Individuais.xlsx"

tempd <- tempdir(check = T) ;temp <- tempfile(tmpdir = tempd)
download.file(url, destfile = temp, quiet = T)

rpi <- readxl::read_xlsx(temp, col_names = F, skip = 2, col_types = "numeric") %>% as.data.frame()
names(rpi) <- c("Ano","Reajuste")

dbWriteTable(con, "RPI", rpi)

unlink(temp, recursive = T) ; unlink(tempd, recursive = T)
rm(url, tempd, temp, rpi)

# 08 - Nota Técnica de Produtos (Vigente para comercialização) ----
url <- "http://ftp.dadosabertos.ans.gov.br/FTP/PDA/nota_tecnica_ntrp_vig_comerc_planos_saude/nota_tecnica_ntrp_planos_saude.csv"
ntrp <- read.csv2(url, header = T, colClasses = "character")
head(ntrp,4)
ntrp <- ntrp[,-c(1,2,8)]
head(ntrp,4)

dbWriteTable(con, "NTRP_VING_COM", ntrp, overwrite = T)

rm(url, ntrp)

# 09 - Nota Técnica de Produtos (Abrangência Geográfica de Comercialização) ----
url <- "http://ftp.dadosabertos.ans.gov.br/FTP/PDA/abrangencia_geografica_comercializacao_planos_ntrp/pda_abra_geo_comer_nrtp.csv"
options(timeout = 6000)

tempd = tempdir(check = T) ; temp = tempfile(tmpdir = tempd)
download.file(url, quiet = T, destfile = temp)
ntrp <- fread(temp, header = T, sep = ";", colClasses = "character")
ntrp <- ntrp[,-c(3,7:9)]

dbWriteTable(con, "NTRP_ABRAN_COM", ntrp, overwrite = T)

unlink(temp, recursive = T) ; unlink(tempd, recursive = T)
rm(temp, tempd, ntrp, url)

# 10 - Terminologia Unificada da Saúde Suplementar (TUSS) ----
url <- "http://ftp.dadosabertos.ans.gov.br/FTP/PDA/terminologia_unificada_saude_suplementar_TUSS/TUSS.zip"

tempd = tempdir(check = T) ; temp = tempfile(tmpdir = tempd)
download.file(url, destfile = temp, quiet = T)

unzip(temp, exdir = tempd)
setwd(paste0(tempd,"/TUSS")) ; tblsTUSS <- list.files(paste0(tempd,"/TUSS"), ".csv")

TUSS <- lapply(tblsTUSS, fread, sep=";", colClasses="character")
TUSS[[1]]

dbSendQuery(con, "CREATE DATABASE IF NOT EXISTS TUSS;")

con1 <- odbc::dbConnect(odbc::odbc(), 
                        driver = "YourDriver", 
                        server = "YourServer",
                        uid = "YourUserID", 
                        pwd = "YourPassword", 
                        port = "YourPort",
                        database = "TUSS")

for (i in 1:length(tblsTUSS)){
  dbWriteTable(con1, toupper(str_sub(tblsTUSS[[i]], 1, 9)), TUSS[[i]], overwrite = T)
  
  print(paste0(str_sub(tblsTUSS[[i]],1,6)," ",str_sub(tblsTUSS[[i]],8,9), " incorporada com sucesso!"))
}

unlink(temp, recursive = T) ; unlink(tempd, recursive = T)
rm(temp, tempd, TUSS, tblsTUSS)

# 11 - Painel de Precificação (NT_VC) ----

# Observações dos dados do Painel de Precificação:
# Os dados de dezembro com relação aos percentuais de carregamentos do preço tem
# que dividir por 100 (valores em percentuais como 10 = 0.1)
# Os dados de Julho já estão em valores absolutos, sem multiplicar por 100.

url <- c("http://ftp.dadosabertos.ans.gov.br/FTP/PDA/painel_de_precificacao/Painel_de_Precificacao_Julho_2020.zip",
         "http://ftp.dadosabertos.ans.gov.br/FTP/PDA/painel_de_precificacao/Painel_de_Precificacao_Dezembro_2020.zip"
)
tempd = tempdir(check = T) ; temp = tempfile(tmpdir = tempd)

for (i in 1:length(url)){
  download.file(url[i], destfile = temp, quiet = T)
  
  direc = strsplit(url[i], "/") %>% extract2(1)
  compet = strsplit(substr(direc[length(direc)],start = 1, stop = nchar(direc[length(direc)])-4),"_")[[1]][c(4,5)]
  
  dir.create(paste0(tempd, "/", substr(direc[length(direc)],
                                       start = 1, 
                                       stop = nchar(direc[length(direc)])-4))
  )
  setwd(paste0(tempd, "/", substr(direc[length(direc)],
                                  start = 1, stop = 
                                    nchar(direc[length(direc)])-4))
  )
  unzip(temp)
  
  NT_VC = read.csv2("nt_vc.csv", header = T)
  
  if(i == 1){ # 1 igual ao primeiro mês: Julho. Necessário efetuar padronização na estrutura dos dados.
    
    NT_VC = NT_VC[,c(1:2, 8:10, 4:7)] # Deixando as colunas de Julho na mesma ordem das colunas de Dezembro
    NT_VC$comp =  "2020/07"
    
  } else if(i == 2){
    
    NT_VC = NT_VC[,-c(3)]
    # Dividindo os carregamentos por 100 para ficar em valor absoluto.
    NT_VC$carreg_total = NT_VC$carreg_total/100;  NT_VC$carreg_adm = NT_VC$carreg_adm/100
    NT_VC$carreg_com = NT_VC$carreg_com/100;      NT_VC$carreg_lucro = NT_VC$carreg_lucro/100
    NT_VC$comp =  "2020/12"
    
  }
  
  # Armazenar os dados no SQL
  if(!c("NT_VC")%in%dbListTables(con)){
    
    dbWriteTable(con, "NT_VC", NT_VC)
    print(paste0("Tabela NT_VC para a competência de ", compet[1], " de ", compet[2], " criada com sucesso!"))
    
  } else {
    
    dbWriteTable(con, "NT_VC", NT_VC, append=T, header=F,row.names=F)
    print(paste0("Competencia de ", compet[1], " de ", compet[2]," incorporada com sucesso!"))
    
  }
}

unlink(temp, recursive = T) ; unlink(tempd, recursive = T); rm(temp, tempd, NT_VC)

# 12 - Abrangência Geográfica dos Planos de Saúde ----

url = "http://ftp.dadosabertos.ans.gov.br/FTP/PDA/abrangencia_geografica_planos_saude/abrangencia_geografica.zip"
direc = strsplit(url, "/") %>% extract2(1) %>% extract(length(.))

temp = tempfile(); tempd = tempdir()

download.file(url, destfile = temp, quiet = T, timeout = 84600)

dir.create(paste0(tempd, "/", substr(direc,
                                     start = 1,
                                     stop = nchar(direc)-4)))
setwd(paste0(tempd, "/", substr(direc,
                                start = 1,
                                stop = nchar(direc)-4)))
unzip(temp)

lst = list.files()
for (i in 1:length(lst)){
  a = fread(lst[i], sep = ";", colClasses = "character", encoding = "Latin-1", drop = c(5:8))
  names(a) = c("ID_PLANO", "CD_PLANO", "CD_OPS", "CD_MUNICIPIO")
  
  # Escrevendo os dados de forma automática caso a tabela não exista e acrescentando linhas caso exista
  if(!"ABR_GEO_PROD"%in%dbListTables(con)){
    dbWriteTable(con, "ABR_GEO_PROD", a)
    print(paste0(str_sub(lst[i], start = 1, end = nchar(lst[i])-4), " incorporada com sucesso!!"))
  } else {
    dbWriteTable(con, "ABR_GEO_PROD", a, append=T, header=F,row.names=F)
    print(paste0(str_sub(lst[i], start = 1, end = nchar(lst[i])-4), " incorporada com sucesso!!"))
  }
}

unlink(temp, recursive = T, force = T); unlink(tempd, recursive = T, force = T)
rm(temp, tempd, direc, url)

# Observações ----
# Dados consolidados até aqui, restando apenas ajustar o código para atualizar de forma autónoma.

