# set-up ------------------------------------------------------------------
lapply(
  c(
    "tidyverse",
    "data.table",
    "here",
    "R.utils",
    "stringi",
    "foreach",
    "doParallel"
  ),
  require, character.only = T
)

# conversion ibge-tse
identifiers <- fread(
  here("files/cod_ibge_tse.csv"),
  colClasses = c("cod_tse" = "character", "cod_ibge_7" = "character")
) %>% 
  select(
    starts_with("cod")
  ) %>% 
  transmute(
    cod_tse,
    cod_ibge_6 = str_sub(cod_ibge_7, 1, 6)
  )

set.seed(1789)

# read-in -----------------------------------------------------------------
# all elections
campaign <- list.files(
  here("data/raw/campaign/receita"),
  pattern = "^receita_.*\\.csv\\.gz",
  full.names = T
) %>% 
  map(
    . %>% 
      gunzip(
        remove = F,
        temporary = T,
        overwrite = T
      ) %>% 
      fread(
        integer64 = "character",
        encoding = "Latin-1",
        nThread = parallel::detectCores() - 1,
        dec = ","
      )
  )

# wrangle -----------------------------------------------------------------
cl <- makeForkCluster(2)
registerDoParallel(cl)
campaign <- foreach(i = 1:length(campaign)) %dopar%{
  # reshape cols
  names(campaign[[i]]) <- names(campaign[[i]]) %>%
    str_replace_all(
      "\"",
      ""
    ) %>% 
    str_to_lower() %>% 
    make.names %>% 
    str_replace_all(
      "\\.",
      "_"
    ) %>% 
    str_replace_all(
      "__",
      "_"
    ) %>% 
    stri_trans_general("latin-ascii")

  if(i == 1){
    campaign[[i]] <- campaign[[i]] %>% 
      transmute(
        cod_uf = sg_uf,
        party = sg_part,
        candidate_name = no_cand,
        candidate_num = nr_cand,
        position = ds_cargo,
        cod_uf_donor = sg_uf_doador,
        cpf_cnpj_donor = cd_cpf_cgc, # note there quite a few missing values
        donor_name = no_doador,
        date_receipt = dt_receita,
        value_receipt = vr_receita,
        type_resource = tp_recurso
      )
  }
  
  if(i == 2){
    # rename cols
    campaign[[i]] <- campaign[[i]] %>% 
      transmute(
        cod_tse = sg_ue,
        party = sg_part,
        candidate_name = no_cand,
        candidate_num = nr_cand,
        position = ds_cargo,
        position_code = cd_cargo,
        donation_status = rv_meaning,
        cpf_cnpj_donor = cd_cpf_cgc_doa, # note there quite a few missing values
        donor_name = no_doador,
        type_source = rtrim_ltrim_dr_ds_titulo_,
        type_resource = decode_rec_tp_recurso_0_emespecie_1_cheque_2_estimado__naoinformado_,
        value_receipt = vr_receita
      )
  }
  
  if(i == 3){
    campaign[[i]] <- campaign[[i]] %>% 
      transmute(
        cod_uf = unidade_eleitoral_candidato,
        party = sigla_partido,
        candidate_name = nome_candidato,
        candidate_num = numero_candidato,
        cpf_candidate = numero_cnpj_candidato,
        position = descricao_cargo,
        position_code = codigo_cargo,
        cpf_cnpj_donor = numero_cpf_cgc_doador,
        mun_donor = unidade_eleitoral_doador,
        donor_name = nome_doador,
        type_source = tipo_receita,
        type_resource = descricao_tipo_recurso,
        value_receipt = valor_receita
      )
  }
  
  if(i == 4){
    campaign[[i]] <- campaign[[i]] %>% 
      transmute(
        cod_tse = sg_ue,
        party = sg_partido,
        candidate_name = nm_candidato,
        candidate_num = nr_candidato,
        cpf_candidate = cd_num_cpf,
        position = ds_cargo,
        position_code = cd_cargo,
        cpf_cnpj_donor = cd_cpf_cnpj_doador,
        cod_tse_donor = sg_ue_1,
        donor_name = nm_doador,
        type_source = ds_titulo,
        type_resource = ds_esp_recurso,
        campaign_manager_name = nm_adm,
        cpf_campaign_manager = cd_cpf_adm,
        date_receipt = dt_receita,
        value_receipt = vr_receita
      )
  }
  
  if(i == 5){
    campaign[[i]] <- campaign[[i]] %>% 
      transmute(
        cod_uf = uf,
        party = sigla_partido,
        candidate_name = nome_candidato,
        candidate_num = numero_candidato,
        position = cargo,
        cpf_candidate = cpf_do_candidato,
        receipt_number = numero_recibo_eleitoral,
        cpf_cnpj_donor = cpf_cnpj_do_doador,
        donor_name = nome_do_doador,
        date_receipt = data_da_receita,
        value_receipt = str_replace(valor_receita, ",", ".") %>% 
          as.numeric,
        type_source = tipo_receita,
        type_resource = tipo_receita,
        desc_resource = especie_recurso
      ) %>% # filter out descriptions of vars
      filter(
        cod_uf != "UF"
      )
  }
  
  if(i == 6){
    campaign[[i]] <- campaign[[i]] %>% 
      select(
        cod_uf = uf,
        cod_tse = numero_ue,
        party = sigla_partido,
        candidate_num = numero_candidato,
        position = cargo,
        cpf_candidate = cpf_do_candidato,
        receipt_number = numero_recibo_eleitoral,
        cpf_cnpj_donor = cpf_cnpj_do_doador,
        donor_name_tax = nome_do_doador_receita_federal_,
        donor_name = nome_do_doador,
        cod_tse_donor = sigla_ue_doador,
        party_donor = numero_partido_doador,
        candidate_num_donor = numero_candidato_doador,
        cod_sector_donor = cod_setor_economico_do_doador,
        desc_sector_donor = setor_economico_do_doador,
        date_receipt = data_da_receita,
        value_receipt = valor_receita,
        origin_resource = fonte_recurso,
        type_source = tipo_receita,
        type_resource = especie_recurso,
        desc_resource = descricao_da_receita
      )
  }
  
  if(i == 7){
    # rename cols
    campaign[[i]] <- campaign[[i]] %>% 
      select(
        election_code = cod_eleicao,
        election_type = desc_eleicao,
        cod_uf = uf,
        party = sigla_partido,
        candidate_num = numero_candidato,
        position = cargo,
        cpf_candidate = cpf_do_candidato,
        receipt_number = numero_recibo_eleitoral,
        cpf_cnpj_donor = cpf_cnpj_do_doador,
        donor_name = nome_do_doador,
        donor_name_tax = nome_do_doador_receita_federal_,
        cod_tse_donor = sigla_ue_doador,
        party_donor = numero_partido_doador,
        candidate_num_donor = numero_candidato_doador,
        cod_sector_donor = cod_setor_economico_do_doador,
        desc_sector_donor = setor_economico_do_doador,
        date_receipt = data_da_receita,
        value_receipt = valor_receita,
        origin_resource = fonte_recurso,
        type_source = tipo_receita,
        type_resource = especie_recurso,
        desc_resource = descricao_da_receita
      )
  }
  
  if(i == 8){
    # rename cols
    campaign[[i]] <- campaign[[i]] %>% 
      select(
        election_code = cod_eleicao,
        election_type = desc_eleicao,
        cod_tse = sigla_da_ue,
        party = sigla_partido,
        candidate_num = numero_candidato,
        position = cargo,
        cpf_candidate = cpf_do_candidato,
        receipt_number = numero_recibo_eleitoral,
        cpf_cnpj_donor = cpf_cnpj_do_doador,
        donor_name = nome_do_doador,
        donor_name_tax = nome_do_doador_receita_federal_,
        cod_tse_donor = sigla_ue_doador,
        party_donor = numero_partido_doador,
        candidate_num_donor = numero_candidato_doador,
        cod_sector_donor = cod_setor_economico_do_doador,
        desc_sector_donor = setor_economico_do_doador,
        date_receipt = data_da_receita,
        value_receipt = valor_receita,
        origin_resource = fonte_recurso,
        type_source = tipo_receita,
        type_resource = especie_recurso,
        desc_resource = descricao_da_receita
      )
  }
  
  # remove special chars
  campaign[[i]] <- campaign[[i]] %>% 
    mutate_if(
      is.character,
      str_to_lower
    ) %>% 
    mutate_at(
      vars(
        contains("name"),
        starts_with("type"),
        starts_with("desc")
      ),
      funs(
        stri_trans_general(., "latin-ascii")
      )
    )
  
  # election_year
  campaign[[i]] <- campaign[[i]] %>% 
    mutate(
      election_year = seq(2002, 2016, 2)[i]
    )
  
  return(campaign[[i]])
}
stopCluster(cl)
rm(cl)

# rbind
campaign <- rbindlist(campaign, fill = T)

# fix cod_tse
campaign <- campaign %>% 
  mutate(
    cod_tse = str_pad(cod_tse, "5", "left", "0"),
    cod_tse_donor = str_pad(cod_tse_donor, "5", "left", "0")
  )

# replace NA's
campaign[campaign == "#nulo"|campaign == "#NULO"] <- NA
campaign[campaign == "#nulo#"|campaign == "#NULO#"] <- NA

# fix defective characters
campaign <- campaign %>% 
  mutate_if(
    is.character,
    function(x) str_replace_all(x, '\\"', "")
  ) %>% 
  mutate_if(
    is.character,
    function(x) str_replace_all(x, '\\\\', "")
  )

# remove defective value entries
# start with a comma and 0 donation
# lose 2k obs.
campaign <- campaign %>%
  filter(
    !str_detect(value_receipt, "^0$")
  )

# fix candidate cpf with candidate data
candidate_id <- gunzip(
  here("data/wrangle/candidate.csv.gz"),
  remove = F,
  temporary = T,
  overwrite = T
) %>% 
  fread(
    colClasses = c("candidate_num" = class(campaign$candidate_num))
  ) %>% 
  distinct(
    cpf_candidate,
    election_year,
    cod_tse,
    state,
    candidate_num,
    candidate_name
  ) %>% 
  filter(
    cpf_candidate != 0
  )

# join with contributors data to fix cpf_candidate
# federal level
campaign <- campaign %>% 
  left_join(
    candidate_id %>%
      filter(election_year %% 4 == 2) %>% 
      select(-cod_tse),
    by = c("cod_uf" = "state", "candidate_name", "candidate_num", "election_year")
  ) %>% 
  mutate(
    cpf_candidate = case_when(
      cpf_candidate.x == ""|is.na(cpf_candidate.x)|str_count(cpf_candidate.x) > 11 ~ cpf_candidate.y,
      T ~ cpf_candidate.x
    )
  ) %>% 
  select(
    -ends_with(".x"),
    -ends_with(".y")
  )

# local level
campaign <- campaign %>% 
  mutate(
    cod_tse = as.character(cod_tse)
  ) %>% 
  left_join(
    candidate_id %>%
      filter(election_year %% 4 == 0) %>% 
      select(-state),
    by = c("cod_tse", "candidate_name", "candidate_num", "election_year")
  ) %>% 
  mutate(
    cpf_candidate = case_when(
      cpf_candidate.x == ""|is.na(cpf_candidate.x)|str_count(cpf_candidate.x) > 11 ~ cpf_candidate.y,
      T ~ cpf_candidate.x
    )
  ) %>% 
  select(
    -ends_with(".x"),
    -ends_with(".y")
  )

# remove defective entries
campaign <- campaign %>% 
  filter(
    str_count(cpf_candidate) > 1,
    str_count(cpf_cnpj_donor) > 1,
    cpf_candidate != "00000000000",
    cpf_cnpj_donor != "00000000000"
  )

# reorder cols
campaign <- campaign %>% 
  arrange(
    election_year
  ) %>% 
  select(
    order(colnames(.))
  ) %>% 
  select(
    cod_uf,
    cod_tse,
    election_year,
    cpf_candidate,
    everything()
  )

# write-out federal and state
campaign_fed_state <- campaign %>% 
  filter(
    election_year %in% seq(2002, 2014, 4)
  )

campaign_fed_state %>%
  fwrite(
    here("data/wrangle/campaign_fed_state.csv")
  )

gzip(
  here("data/wrangle/campaign_fed_state.csv"),
  overwrite = T
)

# write-out local
campaign_local <- campaign %>% 
  filter(
    election_year %in% seq(2000, 2016, 4)
  ) %>% 
  left_join(
    identifiers,
    by = c("cod_tse")
  ) 
  
campaign_local %>% 
  fwrite(
    here("data/wrangle/campaign_local.csv")
  )

gzip(
  here("data/wrangle/campaign_local.csv"),
  overwrite = T
)
