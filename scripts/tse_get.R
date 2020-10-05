# set-up ------------------------------------------------------------------
pacman::p_load(
  "tidyverse",
  "R.utils",
  "data.table",
  "stringi",
  "electionsBR",
  "rvest",
  "here",
  "foreach",
  "stringr",
  "parallel",
  "doParallel",
  "furrr"
)

set.seed(1789)

states <-  c(
  "AC", "AL", "AM", "AP", "BA", "CE", 
  "DF", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", 
  "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", 
  "SE", "SP", "TO"
)

# function ----------------------------------------------------------------
# fix defective function to clean data
juntaDados_fix <- function (uf, encoding){
    Sys.glob(c("*.txt", "*.csv"))[grepl(uf, Sys.glob(c("*.txt", "*.csv")))] %>% 
    lapply(function(x) tryCatch(data.table::fread(x, 
        header = F, sep = ";", stringsAsFactors = F, data.table = F, colClasses = "character", 
        verbose = F, showProgress = F, encoding = encoding), 
        error = function(e) NULL)) %>% data.table::rbindlist() %>% 
        dplyr::as.tbl()
}

# assign
assignInNamespace("juntaDados", juntaDados_fix, "electionsBR")

# modify candidate local function
candidate_local_fix <- function (year, uf = "all", ascii = FALSE, encoding = "Latin-1", 
                                 export = FALSE) {
  test_encoding(encoding)
  test_local_year(year)
  uf <- test_uf(uf)
  dados <- tempfile()
  sprintf("http://agencia.tse.jus.br/estatistica/sead/odsele/consulta_cand/consulta_cand_%s.zip", 
          year) %>% download.file(dados)
  unzip(dados, exdir = paste0("./", year))
  unlink(dados)
  message("Processing the data...")
  setwd(as.character(year))
  banco <- juntaDados(uf, encoding)
  setwd("..")
  unlink(as.character(year), recursive = T)
  if (year < 2012) {
    names(banco) <- c("DATA_GERACAO", "HORA_GERACAO", "ANO_ELEICAO", 
                      "NUM_TURNO", "DESCRICAO_ELEICAO", "SIGLA_UF", "SIGLA_UE", 
                      "DESCRICAO_UE", "CODIGO_CARGO", "DESCRICAO_CARGO", 
                      "NOME_CANDIDATO", "SEQUENCIAL_CANDIDATO", "NUMERO_CANDIDATO", 
                      "CPF_CANDIDATO", "NOME_URNA_CANDIDATO", "COD_SITUACAO_CANDIDATURA", 
                      "DES_SITUACAO_CANDIDATURA", "NUMERO_PARTIDO", "SIGLA_PARTIDO", 
                      "NOME_PARTIDO", "CODIGO_LEGENDA", "SIGLA_LEGENDA", 
                      "COMPOSICAO_LEGENDA", "NOME_COLIGACAO", "CODIGO_OCUPACAO", 
                      "DESCRICAO_OCUPACAO", "DATA_NASCIMENTO", "NUM_TITULO_ELEITORAL_CANDIDATO", 
                      "IDADE_DATA_ELEICAO", "CODIGO_SEXO", "DESCRICAO_SEXO", 
                      "COD_GRAU_INSTRUCAO", "DESCRICAO_GRAU_INSTRUCAO", 
                      "CODIGO_ESTADO_CIVIL", "DESCRICAO_ESTADO_CIVIL", 
                      "CODIGO_NACIONALIDADE", "DESCRICAO_NACIONALIDADE", 
                      "SIGLA_UF_NASCIMENTO", "CODIGO_MUNICIPIO_NASCIMENTO", 
                      "NOME_MUNICIPIO_NASCIMENTO", "DESPESA_MAX_CAMPANHA", 
                      "COD_SIT_TOT_TURNO", "DESC_SIT_TOT_TURNO")
  }
  else if (year == 2012) {
    names(banco) <- c( "DT_GERACAO",                   "HH_GERACAO",                  
                       "ANO_ELEICAO",                  "CD_TIPO_ELEICAO",             
                       "NM_TIPO_ELEICAO",              "NR_TURNO",                    
                       "CD_ELEICAO",                   "DS_ELEICAO",                  
                       "DT_ELEICAO",                   "TP_ABRANGENCIA",              
                       "SG_UF",                        "SG_UE",                       
                       "NM_UE",                        "CD_CARGO",                    
                       "DS_CARGO",                     "SQ_CANDIDATO",                
                       "NR_CANDIDATO",                 "NM_CANDIDATO",                
                       "NM_URNA_CANDIDATO",            "NM_SOCIAL_CANDIDATO",         
                       "NR_CPF_CANDIDATO",             "NM_EMAIL",                    
                       "CD_SITUACAO_CANDIDATURA",      "DS_SITUACAO_CANDIDATURA",     
                       "CD_DETALHE_SITUACAO_CAND",     "DS_DETALHE_SITUACAO_CAND",    
                       "TP_AGREMIACAO",                "NR_PARTIDO",                  
                       "SG_PARTIDO",                   "NM_PARTIDO",                  
                       "SQ_COLIGACAO",                 "NM_COLIGACAO",                
                        "DS_COMPOSICAO_COLIGACAO",      "CD_NACIONALIDADE",            
                        "DS_NACIONALIDADE",             "SG_UF_NASCIMENTO",            
                        "CD_MUNICIPIO_NASCIMENTO",      "NM_MUNICIPIO_NASCIMENTO",     
                        "DT_NASCIMENTO",                "NR_IDADE_DATA_POSSE",         
                        "NR_TITULO_ELEITORAL_CANDIDATO","CD_GENERO",                   
                        "DS_GENERO",                    "CD_GRAU_INSTRUCAO",           
                        "DS_GRAU_INSTRUCAO",            "CD_ESTADO_CIVIL",             
                        "DS_ESTADO_CIVIL",              "CD_COR_RACA",                 
                        "DS_COR_RACA",                  "CD_OCUPACAO",                 
                        "DS_OCUPACAO",                  "NR_DESPESA_MAX_CAMPANHA",     
                        "CD_SIT_TOT_TURNO",             "DS_SIT_TOT_TURNO",            
                        "ST_REELEICAO",                 "ST_DECLARAR_BENS",            
                        "NR_PROTOCOLO_CANDIDATURA",     "NR_PROCESSO")
  }
  else {
    names(banco) <- c()
  }
  if (ascii == T) 
    banco <- to_ascii(banco, encoding)
  if (export) 
    export_data(banco)
  message("Done.\n")
  return(banco)
}

environment(candidate_local_fix) <- as.environment("package:electionsBR")

# elections ---------------------------------------------------------------
# local
elections_local <- seq(2000, 2016, by = 4) %>%
  map(
    function(x) vote_mun_zone_local(year = x)
  ) %>%
  rbindlist(fill = T) %>%
  fwrite("./data/raw/election/election_local.csv")

# compress
gzip(
  "./data/raw/election/election_local.csv",
  overwrite = T
)

# vote by party
party_local <- seq(2000, 2016, by = 4) %>%
  map(
    function(x) party_mun_zone_local(year = x)
  ) %>%
  rbindlist(fill = T) %>%
  fwrite("./data/raw/election/party_local.csv")

# verified results
details_local <- seq(2000, 2016, by = 4) %>%
  map(
    function(x) details_mun_zone_local(
      year = x,
      uf = c(
        "AC", "AL", "AM", "AP", "BA", "CE", 
        "DF", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", 
        "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", 
        "SE", "SP", "TO"
      )
    )
  ) %>%
  rbindlist(fill = T) %>%
  fwrite("data/raw/election/details_local.csv")

# federal
elections_fed_state <- seq(2002, 2014, 4) %>%
  map(
    function(x) vote_mun_zone_fed(year = x)
  ) %>%
  rbindlist(fill = T) %>%
  fwrite("./data/raw/election/election_fed_state.csv")

# compress
gzip(
  "./data/raw/election/election_fed_state.csv"
)

# vote by party
party_fed_state <- seq(2002, 2014, 4) %>%
  map(
    function(x) party_mun_zone_fed(year = x)
  ) %>%
  rbindlist(fill = T)

party_fed_state %>%
  fwrite("./data/raw/election/party_fed_state.csv")

# verified results
details_fed_state <- seq(2002, 2016, by = 4) %>%
  map(
    function(x) details_mun_zone_fed(
      year = x, 
      uf = states
    )
  ) %>%
  rbindlist(
    fill = T
  ) %>% 
  fwrite("./data/raw/election/details_fed_state.csv")

# candidate ---------------------------------------------------------------
# local candidates
# make sure to fix juntaDados: set colClasses = "character"
candidate_mun <- seq(2000, 2016, 4) %>%
  map(
    function(x) candidate_local(
      year = x,
      uf = states
    )
  ) %>%
  rbindlist(fill = T)

candidate_mun %>%
  fwrite("./data/raw/election/candidate_local.csv")

# compress
gzip(
  "./data/raw/election/candidate_local.csv"
)

# state candidates
candidate_state_fed <- seq(1998, 2014, 4) %>%
  map(
    function(x) candidate_fed(year = x)
  ) %>%
  rbindlist(fill = T) %>%
  fwrite("./data/raw/election/candidate_fed_state.csv")

# compress
gzip(
  "./data/raw/election/candidate_fed_state.csv"
)

# coalitions --------------------------------------------------------------
coalition_local <- seq(2000, 2016, 4) %>% 
  map(
    function(x) legend_local(x)
  ) %>% 
  rbindlist(
    fill = T
  )

coalition_fed <- seq(2002, 2014, 4) %>% 
  map(
    function(y) legend_fed(y)
  ) %>% 
  rbindlist(
    fill = T
  )

# fix characters
coalition <- rbindlist(
  list(
    coalition_local,
    coalition_fed
  ),
  fill = T
) 

# write-out
coalition %>% 
  fwrite(
    "data/raw/election/coalition.csv"
  )

# filiados ----------------------------------------------------------------
party <- parties_br()

uf <- uf_br()

# load all parties from all states
plan(multicore)
filiados <- future_pmap(
  crossing(party = party, uf = uf),
  possibly(voter_affiliation, NULL)
) %>%
  rbindlist

filiados %>%
  fwrite(
    here("data/raw/filiados.csv.gz"),
    compress = "gzip"
  )