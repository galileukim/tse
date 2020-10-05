library(tidyverse)
library(here)
library(lubridate)

fread <- data.table::fread %>%
    partial(
        nThread = parallel::detectCores(),
        integer64 = "character"
    )

here <- here::here

cod_ibge_tse <- fread(
    here("files/cod_ibge_tse.csv")
) %>%
    transmute(
        cod_ibge_6 = str_sub(cod_ibge_7, 1, 6),
        cod_tse
    )

filiado <- fread(
    here("data/raw/filiados.csv.gz")
)

candidato <- fread(
    here("data/wrangle/candidate.csv.gz")
)

# clean-up columns and add municipal identifiers from ibge
# finally correct dates, add years and convert to lower
filiado_clean <- filiado %>%
    rename_with(
        str_to_lower
    ) %>%
    left_join(
        cod_ibge_tse,
        by = c("codigo_do_municipio" = "cod_tse")
    ) %>%
    transmute(
        cod_ibge_6,
        state = uf,
        mun_name = nome_do_municipio,
        electoral_zone = zona_eleitoral,
        electoral_section = secao_eleitoral,
        # fix length of electoral title to match filiado
        electoral_title = str_pad(numero_da_inscricao, 12, "left", "0"),
        member_name = nome_do_filiado,
        party = sigla_do_partido,
        date_record_extraction = data_da_extracao,
        date_start = data_da_filiacao,
        date_end = data_da_desfiliacao,
        date_cancel = data_do_cancelamento,
        reason_cancel = motivo_do_cancelamento,
        member_status = situacao_do_registro
    ) %>%
    mutate(
        across(
            where(is.character),
            str_to_lower
        ),
        across(
            starts_with("date"),
            dmy
        ),
        across(
            starts_with("date"),
            list(year = lubridate::year),
            .names = "{fn}_{col}"
        ),
        year_termination = pmax(year_end, year_cancel, na.rm = T) %>%
            replace_na(2019)
    ) %>%
    rename_with(
        ~ stringr::str_replace(., "year_date", "year")
    )

# join data with candidate data to incorporate info on cpf
defective_cpf <- c("", 0, str_dup(0, 11), str_dup(9, 11))

candidato_cpf <- candidato %>% # filter out defective cpf entries
    filter(
        !(cpf_candidate %in% defective_cpf)
    ) %>%
    distinct(
        elec_title,
        cpf_candidate,
        candidate_name
    ) %>%
    rename(
        electoral_title = elec_title
    ) %>%
    mutate(
        electoral_title = str_pad(
            electoral_title,
            12,
            "left",
            "0"
        )
    )

filiado_cpf <- filiado_clean %>%
    left_join(
        candidato_cpf,
        by = c("electoral_title")
    )

filiado_cpf %>%
    data.table::fwrite(
        here::here("data/wrangle/filiado.csv.gz"),
        compress = "gzip"
    )