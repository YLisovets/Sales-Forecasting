connect_to_db <- function() {
    
    con_db <- dbConnect(odbc(),
                        Driver = "SQL Server", #"FreeTDS"
                        Server = Sys.getenv("SERVER_NAME"),
                        Database = Sys.getenv("DATABASE_NAME"),
                        UID = Sys.getenv("UID_DATABASE"),
                        PWD = Sys.getenv("PWD_DATABASE")
    )
    
}

con_db <- connect_to_db()

get_item_main_parent <- function(ref_df) {
    
    item_df <- ref_df[,1]
    
    current_rec <- left_join(item_df,
                             select(ref_df, item_id, parent_last = parent_itemId)) 
    i <- 1
    row_qty <- nrow(current_rec)
    while(sum(is.na(current_rec[, ncol(current_rec)])) != row_qty) {
        prev_parent <- current_rec
        current_rec <- left_join(prev_parent,
                                 select(ref_df, item_id, parent_itemId),
                                 by = c("parent_last" = "item_id"))
        names(current_rec)[ncol(current_rec) - 1] <- stringr::str_glue("parent_level_{i}")
        names(current_rec)[ncol(current_rec)] <- "parent_last"
        i <- i + 1
    }
    
    res <- current_rec %>% 
        mutate(main_parent = coalesce(!!!rev(select(., -item_id)))) %>% 
        # mutate(last_non_na = max.col(!is.na(current_rec), ties.method = "last")) %>%
        select(main_parent)
    
    return(res)
    
}


# Отримання довідників
get_ref_items <- function() {
    
    tbl_item <- tbl(con_db, "_Reference120") %>%
        select(
            "_IDRRef",
            item_id = "_Code",
            item_name = "_Description",
            "_Fld2045RRef",
            "_ParentIDRRef",
            is_tva = "_Fld2068",
            item_marked = "_Marked"
        ) %>%
        mutate(is_tva = as.logical(is_tva),
               item_marked = as.logical(item_marked))

    tbl_group <- tbl(con_db, "_Reference122") %>%
        select(
            "_IDRRef",
            group_id = "_Code",
            group_name = "_Description")


    ref_items <- tbl_item %>%
        left_join(
            select(tbl_item, "_IDRRef", parent_itemId = item_id),
            by = c("_ParentIDRRef" = "_IDRRef")
        ) %>%
        left_join(tbl_group, by = c("_Fld2045RRef" = "_IDRRef")) %>%
        select(
            item_id,
            item_name,
            group_id,
            is_tva,
            item_marked,
            parent_itemId) %>% 
        collect()
    
    ref_items <- ref_items %>% 
        bind_cols(get_item_main_parent(.))

}


get_ref_subdiv <- function() {
    
    tbl_subdiv <- tbl(con_db, "_Reference135") %>% 
        select("_IDRRef", subdiv_id = "_Code",
               subdiv_name = "_Description",
               subdiv_marked = "_Marked",
               "_ParentIDRRef") %>% 
        mutate(subdiv_marked = as.logical(subdiv_marked))
    
    tbl_object_property_value_rg <- tbl(con_db, "_InfoRg14478") %>%
        select("_Fld14479_RRRef", property = "_Fld14480RRef",
               value = "_Fld14481_RRRef")
    
    tbl_character_types <- tbl(con_db, "_Chrc806") %>%
        select("_IDRRef", code = "_Code")
    
    tbl_object_property_value_ref <- tbl(con_db, "_Reference85") %>%
        select("_IDRRef", name = "_Description")
    
    ref_subdiv <- tbl_subdiv %>% 
        left_join(select(tbl_subdiv, "_IDRRef", parent_subdiv = subdiv_id),
                  by = c("_ParentIDRRef" = "_IDRRef")) %>% 
        left_join(tbl_object_property_value_rg,
                  by = c("_IDRRef" = "_Fld14479_RRRef")) %>%
        inner_join(tbl_character_types, by = c(property = "_IDRRef")) %>%
        filter(code == "000000191") %>%
        left_join(tbl_object_property_value_ref, by = c(value = "_IDRRef")) %>%
        select(subdiv_id, subdiv_name, parent_subdiv, subdiv_marked, 
               subdiv_category = name) %>% 
        collect()
    
}


## Реализация товаров и услуг полная
get_sale_total <- function(date_start, date_finish) {

    date_start <- as_datetime(date_start + years(2000))
    date_finish <- as_datetime(date_finish + years(2000))
    
    tbl_sale_reg <- tbl(con_db,"_AccumRg17435") %>% 
        select(date = "_Period", active = "_Active",
               # link_organiz = "_Fld17443RRef",
               link_subdiv = "_Fld17441RRef",
               # link_sale_doc = "_Fld17440_RRRef",
               link_customer = "_Fld17444RRef",
               link_project = "_Fld17442RRef",
               link_item = "_Fld17436RRef",
               item_qty = "_Fld17445", item_sum = "_Fld17446",
               item_vat = "_Fld17448"
               # item_sum_without_disc = "_Fld17446"
               ) %>% 
        mutate(active = as.logical(active)) %>% 
        filter(date >= date_start,
               date < date_finish,
               active == "TRUE")
   
    tbl_project <- tbl(con_db, "_Reference143") %>% 
        select("_IDRRef", project_id = "_Code", parent_project = "_ParentIDRRef")
    tbl_subdiv <- tbl(con_db, "_Reference135") %>% 
        select("_IDRRef", subdiv_id = "_Code")
    tbl_item <- tbl(con_db, "_Reference120") %>% 
        select("_IDRRef", item_id = "_Code", parent_item = "_ParentIDRRef")
    tbl_partner <- tbl(con_db, "_Reference102") %>% 
        select("_IDRRef", customer_id = "_Code",
               link_main_cust = "_Fld21315RRef"
        )
    
    df_sale <- tbl_sale_reg %>%
        left_join(tbl_subdiv,  by = c(link_subdiv = "_IDRRef")) %>%
        left_join(tbl_project, by = c(link_project = "_IDRRef")) %>%
        left_join(tbl_item,    by = c(link_item = "_IDRRef")) %>%
        left_join(select(tbl_item, "_IDRRef", parent_id = item_id),
                  by = c(parent_item = "_IDRRef")) %>%
        left_join(tbl_partner, by = c(link_customer = "_IDRRef")) %>% 
        left_join(select(tbl_partner, "_IDRRef", main_cust_id = customer_id),
                  by = c(link_main_cust = "_IDRRef")) %>% 
        mutate(date = DATEADD(sql("month"),
                              DATEDIFF(sql("month"), 0, date),
                              0),
               project = case_when(
                   !parent_id %in% "000000185" &           # Послуги надані
                       project_id %in% c("000000002", "000000003") ~ "B2C",
                   project_id %in% c("000000004")                  ~ "rent",
                   parent_id %in% "000000185"                      ~ "deductions",
                   .default = "B2B"
               ),
               customer_id = ifelse(!is.na(customer_id) & !is.na(main_cust_id),
                                    main_cust_id,
                                    customer_id)) %>%
        group_by(date, project, subdiv_id) %>% 
        summarise(sale_sum = sum(item_sum, na.rm = TRUE) +
                             sum(item_vat, na.rm = TRUE),
                  b2b_customer_qty = n_distinct(customer_id[project %in% "B2B"])) %>% 
        ungroup() %>% 
        collect()
    
    return(df_sale)
    
    dbDisconnect(con)
}


## Робочі години магазинів факт (за місяць)
get_working_time <- function(date_start, date_finish) {
    
    date_start <- as_datetime(date_start) + years(2000)
    date_finish <- as_datetime(date_finish) + years(2000)
    
    tbl_check_doc <- tbl(con_db,"_Document329") %>%
        select("_IDRRef", nmbr_check_doc = "_Number",
               date = "_Date_Time",
               link_check_subdiv = "_Fld7966RRef", is_posted = "_Posted") %>% 
        mutate(is_posted = as.logical(is_posted)) %>% 
        filter(is_posted == "TRUE",
               date >= date_start,
               date  < date_finish)
    
    tbl_check_doc_tbl <- tbl(con_db,"_Document329_VT7992") %>%
        select("_Document329_IDRRef", time = "_Fld8020")
    tbl_subdiv <- tbl(con_db, "_Reference135") %>%
        select("_IDRRef", subdiv_id = "_Code")
    
    
    data <- tbl_check_doc %>% 
        left_join(tbl_check_doc_tbl , by = c("_IDRRef" = "_Document329_IDRRef")) %>% 
        filter(time != "") %>% 
        left_join(tbl_subdiv, by = c(link_check_subdiv = "_IDRRef")) %>% 
        group_by(nmbr_check_doc, date, subdiv_id) %>% 
        filter(time == max(time) | time == min(time)) %>% 
        ungroup() %>% 
        mutate(day = as.Date(date)) %>% 
        group_by(day, subdiv_id) %>%
        summarise(min_time = min(time),
                  max_time = max(time),
                  time_doc = max(date)) %>%
        ungroup() %>%
        collect()
    
    result_df <- data %>% 
        mutate(doc_time = hms::as_hms(time_doc),
               time_diff = as.integer(difftime(doc_time, hms::as_hms(max_time), 
                                               units = "hours"))) %>% 
        mutate(date = floor_date(as.Date(day) - years(2000), unit = "month"),
               time_max = ifelse(time_diff < 0 | (time_diff >= 2 &
                                                      hour(doc_time) >= 20),
                                 hms::round_hms(hms::as_hms(max_time), 3600),
                                 hms::round_hms(hms::as_hms(doc_time), 3600)),
               duration = as.integer(difftime(time_max,
                                              hms::round_hms(hms::as_hms(min_time),
                                                             3600),
                                              units = "hours"))
        ) %>% 
        group_by(subdiv_id, date) %>% 
        summarise(store_working_time = sum(duration))
    
    return(result_df)
    
}


## Робочі години магазинів план (згідно графіку)
get_working_time_plan <- function() {
    
    tbl_schedule_work_time <- tbl(con_db,"_Reference24140") %>%
        select("_IDRRef", 
               link_subdiv = "_Fld24141RRef")
    
    tbl_schedule_work_time_tbl <- tbl(con_db,"_Reference24140_VT24143") %>%
        select("_Reference24140_IDRRef", 
               link_day_week = "_Fld24145RRef",
               time_start = "_Fld24146",
               time_finish = "_Fld24147")
    tbl_subdiv <- tbl(con_db, "_Reference135") %>% 
        select("_IDRRef", subdiv_id = "_Code")
    tbl_day_week <- tbl(con_db, "_Enum609") %>%
        select("_IDRRef", day_id = "_EnumOrder")
    
    
    data <- tbl_schedule_work_time %>% 
        left_join(tbl_schedule_work_time_tbl ,
                  by = c("_IDRRef" = "_Reference24140_IDRRef")) %>% 
        left_join(tbl_subdiv, by = c(link_subdiv = "_IDRRef")) %>% 
        left_join(tbl_day_week,  by = c(link_day_week = "_IDRRef")) %>%
        select(subdiv_id, day_id, time_start, time_finish) %>%
        collect() %>% 
        filter(!is.na(subdiv_id)) %>% 
        mutate(day_id = day_id + 1,
               working_time = as.integer(difftime(hms::as_hms(time_finish),
                                                  hms::as_hms(time_start),
                                                  units = "hours"))
        )
    
    return(data)
    
}


## Продажи по розничным чекам
get_checks_data <- function(date_start, date_finish) {
    
    date_start <- as_datetime(date_start + years(2000))
    date_finish <- as_datetime(date_finish + years(2000))
    
    tbl_checks <- tbl(con_db, "_Document329") %>% 
        select("_IDRRef", posted = "_Posted", date = "_Date_Time",
               is_managerial = "_Fld7964",
               nmbr_check_doc = "_Number", "_Fld7966RRef",
               checks_qty = "_Fld7990") %>%
        mutate(posted = as.logical(posted),
               is_managerial = as.logical(is_managerial)) %>%
        filter(date >= date_start,
               date < date_finish,
               posted == "TRUE",
               is_managerial == "TRUE")

    tbl_subdiv <- tbl(con_db, "_Reference135") %>% 
        select("_IDRRef", subdiv_id = "_Code", subdiv_name = "_Description")

    df_checks <- tbl_checks %>%
        left_join(tbl_subdiv, by = c("_Fld7966RRef" = "_IDRRef")) %>%
        mutate(date = DATEADD(sql("month"),
                              DATEDIFF(sql("month"), 0, date),
                              0)) %>% 
        group_by(date, subdiv_id) %>% 
        summarise(checks_qty = sum(checks_qty)) %>% 
        ungroup() %>% 
        collect()
    
    return(df_checks)
}
