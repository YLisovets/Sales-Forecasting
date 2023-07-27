connect_to_db <- function() {
    
    con_db <- dbConnect(odbc(),
                        Driver = "SQL Server",
                        Server = Sys.getenv("SERVER_NAME"),
                        Database = Sys.getenv("DATABASE_NAME"),
                        UID = Sys.getenv("UID_DATABASE"),
                        PWD = Sys.getenv("PWD_DATABASE")
    )
    
}


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
            pack_qty = "_Fld20858",
            "_Fld2048RRef",
            "_Fld2045RRef",
            "_ParentIDRRef",
            "_Fld2060RRef",
            is_tva = "_Fld2068",
            item_marked = "_Marked",
            link_unit_size = "_Fld2039RRef"
        ) %>%
        mutate(is_tva = as.logical(is_tva),
               item_marked = as.logical(item_marked))

    tbl_price_group <- tbl(con_db, "_Reference235") %>%
        select("_IDRRef", price_group = "_Description")

    tbl_group <- tbl(con_db, "_Reference122") %>%
        select(
            "_IDRRef",
            group_id = "_Code",
            group_name = "_Description",
            "_Fld19599RRef",
            "_Fld18403RRef",
            "_ParentIDRRef",
            group_marked = "_Marked"
        ) %>%
        mutate(group_marked = as.logical(group_marked))

    tbl_unit_size <- tbl(con_db, "_Reference84") %>%
        select(
            "_IDRRef",
            width = "_Fld18500",
            height = "_Fld18501",
            depth = "_Fld18499",
            weight = "_Fld1642",
            volume = "_Fld1643"
        )

    ref_items <<- tbl_item %>%
        left_join(
            select(tbl_item, "_IDRRef", parent_itemId = item_id),
            by = c("_ParentIDRRef" = "_IDRRef")
        ) %>%
        left_join(tbl_price_group, by = c("_Fld2060RRef" = "_IDRRef")) %>%
        left_join(tbl_group, by = c("_Fld2045RRef" = "_IDRRef")) %>%
        left_join(tbl_unit_size, by = c(link_unit_size = "_IDRRef")) %>%
        select(
            item_id,
            item_name,
            price_group,
            group_id,
            pack_qty,
            is_tva,
            item_marked,
            parent_itemId,
            width,
            height,
            depth,
            weight,
            volume
        ) %>%
        collect()
    
    ref_items <- ref_items %>% 
        bind_cols(get_item_main_parent(.))

}


## Реализация товаров и услуг полная
get_sale_total <- function(date_start, date_finish) {

    date_start <- as_datetime(date_start + years(2000))
    date_finish <- as_datetime(date_finish + years(2000))
    
    tbl_sale_reg <- tbl(con_db,"_AccumRg17435") %>% 
        select(date = "_Period", active = "_Active",
               link_organiz = "_Fld17443RRef",
               # link_subdiv = "_Fld17441RRef",
               # link_sale_doc = "_Fld17440_RRRef",
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
    tbl_organiz <- tbl(con_db, "_Reference128") %>% 
        select("_IDRRef", organiz_id = "_Code")
    tbl_item <- tbl(con_db, "_Reference120") %>% 
        select("_IDRRef", item_id = "_Code", parent_item = "_ParentIDRRef")
    
    df_sale <- tbl_sale_reg %>%
        left_join(tbl_organiz,  by = c(link_organiz = "_IDRRef")) %>%
        left_join(tbl_project,  by = c(link_project = "_IDRRef")) %>%
        left_join(tbl_item,     by = c(link_item = "_IDRRef")) %>%
        left_join(select(tbl_item, "_IDRRef", parent_id = item_id),
                  by = c(parent_item = "_IDRRef")) %>%
        mutate(date = as_date(date),
               organiz_id = ifelse(organiz_id == "000000001",
                                   "korvet",
                                   "other"),
               project = case_when(
                   !parent_id %in% "000000185" &           # Послуги надані
                       project_id %in% c("000000002", "000000003") ~ "B2C",
                   project_id %in% c("000000004")                  ~ "rent",
                   parent_id %in% "000000185"                      ~ "deductions",
                   .default = "B2B"
               )) %>%
        group_by(date, project, organiz_id) %>% 
        summarise(sale_sum = sum(item_sum, na.rm = TRUE) +
                             sum(item_vat, na.rm = TRUE)) %>% 
        ungroup() %>% 
        collect()
    
    return(df_sale)
    
    dbDisconnect(con)
}


## Прочие доходы (регистр накопления "Доходы")
get_other_income <- function(date_start, date_finish) {

    date_start <- as_datetime(date_start + years(2000))
    date_finish <- as_datetime(date_finish + years(2000))
    
    tbl_other_income <- tbl(con_db,"_AccumRg17913") %>% 
        select(date = "_Period", active = "_Active",
               link_debt_adjust = "_RecorderRRef",
               link_subdiv = "_Fld17914RRef", link_income_item = "_Fld17915RRef",
               income_item_sum = "_Fld17916", income_item_vat = "_Fld17917") %>% 
        mutate(active = as.logical(active)) %>% 
        filter(date >= date_start,
               date < date_finish,
               active == "TRUE")
    
    tbl_income_item <- tbl(con_db, "_Reference168") %>% 
        select("_IDRRef", income_item_id = "_Code",
               income_item_name = "_Description")

    
    df_other_income <- tbl_other_income %>%
        left_join(tbl_income_item, by = c(link_income_item = "_IDRRef")) %>%
        mutate(date = as_date(date)) %>% 
        group_by(date, income_item_id) %>% 
        summarise(sale_sum = sum(income_item_sum, na.rm = TRUE) +
                      sum(income_item_vat, na.rm = TRUE)) %>% 
        ungroup() %>% 
        collect() %>% 
        mutate(project = case_when(
            income_item_id %in% "000001007" ~ "Відсотки отримані",
            income_item_id %in% "000001035" ~ "Курсові різниці",
            income_item_id %in% "000001021" ~ "Бонуси постач.",
            income_item_id %in% "000001027" ~ "Списана кредит.заборг.",
            .default = "Інше"
        ))
    
    return(df_other_income)
    
    dbDisconnect(con)
}


## Себестоимость проданых товаров
get_cost_data <- function(date_start, date_finish) {

    date_start <- as_datetime(date_start + years(2000))
    date_finish <- as_datetime(date_finish + years(2000))
    
    tbl_cost <- tbl(con_db,"_AccumRg17487") %>% 
        select(date = "_Period",
               # link_doc = "_RecorderRRef",
               # link_project = "_Fld17493RRef",
               item_sum = "_Fld17495",
               item_vat = "_Fld17496") %>% 
        filter(date >= date_start,
               date < date_finish)

    # tbl_project <- tbl(con_db, "_Reference143") %>% 
    #     select("_IDRRef", project_id = "_Code", project = "_Description")
    # tbl_sale_doc <- tbl(con_db,"_Document375") %>% 
    #     select("_IDRRef", nmbr_sale_doc = "_Number",
    #            link_sale_organiz = "_Fld10899RRef")
    # tbl_refund_doc <- tbl(con_db,"_Document251") %>% 
    #     select("_IDRRef", nmbr_refund_doc = "_Number",
    #            link_refund_organiz = "_Fld3906RRef")
    # tbl_check_doc <- tbl(con_db,"_Document329") %>% 
    #     select("_IDRRef", nmbr_check_doc = "_Number",
    #            link_check_organiz = "_Fld7962RRef")
    # tbl_organiz <- tbl(con_db, "_Reference128") %>% 
    #     select("_IDRRef", organiz_id = "_Code")
    
    df_cost <- tbl_cost %>%
        # left_join(tbl_project,     by = c(link_project = "_IDRRef")) %>%
        # left_join(tbl_sale_doc,    by = c(link_doc = "_IDRRef")) %>%
        # left_join(select(tbl_organiz, "_IDRRef", sale_doc_organiz = organiz_id),
        #           by = c(link_sale_organiz = "_IDRRef")) %>%
        # left_join(tbl_refund_doc,  by = c(link_doc = "_IDRRef")) %>%
        # left_join(select(tbl_organiz, "_IDRRef", return_organiz = organiz_id),
        #           by = c(link_refund_organiz = "_IDRRef")) %>%
        # left_join(tbl_check_doc,   by = c(link_doc = "_IDRRef")) %>%
        # left_join(select(tbl_organiz, "_IDRRef", check_organiz = organiz_id),
        #           by = c(link_check_organiz = "_IDRRef")) %>%
        # mutate(organiz_id = case_when(
        #     !is.na(check_organiz)    ~ check_organiz,
        #     !is.na(sale_doc_organiz) ~ sale_doc_organiz,
        #     !is.na(return_organiz)   ~ return_organiz
        # )) %>%
        
        mutate(date = as_date(date)) %>% 
        group_by(date) %>% 
        summarise(cost_sum = sum(item_sum, na.rm = TRUE) +
                      sum(item_vat, na.rm = TRUE)) %>% 
        ungroup() %>% 
        collect()
    
    return(df_cost)
    
    dbDisconnect(con)
}


## Сальдо на дату по певному рахунку Корвет
account_balance <- function(date_finish, account = "6412",
                            organization = "000000001") {
    
    date_finish <- as_datetime(date_finish + years(2000))
    
    tbl_acc_credit <- tbl(con_db, "_AccRg855") %>% 
        select("_AccountCtRRef", date = "_Period", active = "_Active",
               link_organization = "_Fld856RRef", moving_sum = "_Fld859") %>% 
        mutate(active = as.logical(active)) %>% 
        filter(date < date_finish,
               active == "TRUE")
    tbl_acc_debet <- tbl(con_db, "_AccRg855") %>% 
        select("_AccountDtRRef", date = "_Period", active = "_Active",
               link_organization = "_Fld856RRef", moving_sum = "_Fld859") %>%
        mutate(active = as.logical(active)) %>%
        filter(date < date_finish,
               active == "TRUE")
    tbl_account <- tbl(con_db, "_Acc18") %>% 
        select("_IDRRef", acc_id = "_Code")
    tbl_organization <- tbl(con_db, "_Reference128") %>% 
        select("_IDRRef", organization_id = "_Code")
    
    acc_ct <- tbl_acc_credit %>% 
        left_join(tbl_account,      by = c("_AccountCtRRef" = "_IDRRef")) %>% 
        left_join(tbl_organization, by = c(link_organization = "_IDRRef")) %>%
        filter(organization_id == organization,
               acc_id == account) %>% 
        summarise(ct_sum = sum(moving_sum)) %>% 
        collect() %>% 
        pull
    
    acc_dt <- tbl_acc_debet %>% 
        left_join(tbl_account,      by = c("_AccountDtRRef" = "_IDRRef")) %>% 
        left_join(tbl_organization, by = c(link_organization = "_IDRRef")) %>%
        filter(organization_id == organization,
               acc_id == account) %>% 
        summarise(ct_sum = sum(moving_sum)) %>% 
        collect() %>% 
        pull()
    
    balance <- acc_dt - acc_ct
    
    return(balance)
    
}


## Рух по рахунку Корвет
account_balance_motion <- function(date_start, date_finish, account = "6412") {
    
    date_start <- as_datetime(date_start + years(2000))
    date_finish <- as_datetime(date_finish + years(2000))
    
    tbl_acc_credit <- tbl(con_db, "_AccRg855") %>% 
        select("_AccountCtRRef", date = "_Period", active = "_Active",
               link_organization = "_Fld856RRef", moving_sum = "_Fld859") %>% 
        mutate(active = as.logical(active)) %>% 
        filter(date <  date_finish,
               date >= date_start,
               active == "TRUE")
    tbl_acc_debet <- tbl(con_db, "_AccRg855") %>% 
        select("_AccountDtRRef", date = "_Period", active = "_Active",
               link_organization = "_Fld856RRef", moving_sum = "_Fld859") %>%
        mutate(active = as.logical(active)) %>%
        filter(date <  date_finish,
               date >= date_start,
               active == "TRUE")
    tbl_account <- tbl(con_db, "_Acc18") %>% 
        select("_IDRRef", acc_id = "_Code")
    tbl_organization <- tbl(con_db, "_Reference128") %>% 
        select("_IDRRef", organization_id = "_Code")
    
    acc_ct <- tbl_acc_credit %>% 
        left_join(tbl_account,      by = c("_AccountCtRRef" = "_IDRRef")) %>% 
        left_join(tbl_organization, by = c(link_organization = "_IDRRef")) %>%
        filter(organization_id == "000000001",
               acc_id == account) %>% 
        mutate(date = DATEADD(sql("quarter"),
                              DATEDIFF(sql("quarter"), 0, date),
                              0)) %>% 
        group_by(date) %>% 
        summarise(ct_sum = sum(moving_sum))
        
    acc_dt <- tbl_acc_debet %>% 
        left_join(tbl_account,      by = c("_AccountDtRRef" = "_IDRRef")) %>% 
        left_join(tbl_organization, by = c(link_organization = "_IDRRef")) %>%
        filter(organization_id == "000000001",
               acc_id == account) %>% 
        mutate(date = DATEADD(sql("quarter"),
                              DATEDIFF(sql("quarter"), 0, date),
                              0)) %>%
        group_by(date) %>% 
        summarise(dt_sum = sum(moving_sum))
    
    saldo <- full_join(acc_ct, acc_dt, by = "date") %>% 
        replace_na(list(ct_sum = 0, dt_sum =0)) %>% 
        mutate(saldo = dt_sum - ct_sum) %>% 
        select(date, saldo) %>% 
        collect()

    return(saldo)

}


get_expenses_data <- function(date_start, date_finish) {

    date_start <- as_datetime(date_start + years(2000))
    date_finish <- as_datetime(date_finish + years(2000))
    
    tbl_expenses <- tbl(con_db,"_AccumRg17131") %>% 
        select(date = "_Period", active = "_Active", type_exp = "_RecordKind",
               link_subdiv = "_Fld17132RRef", link_expItem = "_Fld17133RRef",
               link_project = "_Fld21330RRef",
               exp_sum = "_Fld17136", exp_vat = "_Fld17137") %>% 
        mutate(active = as.logical(active)) %>% 
        filter(active == "TRUE",
               date >= date_start,
               date < date_finish)
    
    tbl_expItem <- tbl(con_db,"_Reference169") %>% 
        select("_IDRRef", expItems_id = "_Code", expItems_name = "_Description",
               "_ParentIDRRef")
    
    df_expenses <- tbl_expenses %>%
        left_join(tbl_expItem, by = c(link_expItem = "_IDRRef")) %>%
        left_join(select(tbl_expItem, "_IDRRef", parent_exp_id = expItems_id,
                         parent_exp_name = expItems_name),
                  by = c("_ParentIDRRef" = "_IDRRef")) %>% 
        
        mutate(date = DATEADD(sql("quarter"),
                              DATEDIFF(sql("quarter"), 0, date),
                              0),
               exp_sum = ifelse(type_exp == 0,
                                exp_sum + exp_vat,
                                -(exp_sum + exp_vat))
               )%>%

        group_by(date, expItems_id, parent_exp_id, parent_exp_name) %>% 
        summarise(exp_sum = sum(exp_sum)) %>%
        ungroup() %>% 
        
        collect()
    
    return(df_expenses)
    
}


get_current_inventory <- function() {
    
    date_today <- as_datetime(today() + years(2000))
    
    tbl_store <- tbl(con_db, "_Reference156") %>%
        select("_IDRRef", link_parent_store = "_ParentIDRRef", marked = "_Marked",
               store_id = "_Code") %>% 
        mutate(marked = as.logical(marked))
    tbl_items <- tbl(con_db, "_Reference120") %>% 
        select("_IDRRef", item_id = "_Code")
    
    
    tbl_stock_balance <- tbl(con_db,"_AccumRgT17710") %>% 
        select(date_balance = "_Period", link_store = "_Fld17703RRef",
               link_item = "_Fld17704RRef", item_store_qty = "_Fld17708"
        ) %>% 
        filter(date_balance > date_today) %>% 
        left_join(tbl_items, by = c(link_item = "_IDRRef")) %>% 
        left_join(tbl_store, by = c(link_store = "_IDRRef")) %>% 
        left_join(select(tbl_store, "_IDRRef", parent_store = store_id),
                  by = c(link_parent_store = "_IDRRef")) %>%
        filter(!parent_store %in% c("000001096",         # Інші закриті
                                    "000001084")) %>%    # Обладнання
        
        group_by(store_id, item_id) %>%
        summarise(item_store_qty = sum(item_store_qty)) %>%
        ungroup() %>%
        filter(item_store_qty != 0) %>%
        collect()
    
    return(tbl_stock_balance)
    
}


get_inventory_movement <- function(date_start) {
    
    date_start <- as_datetime(date_start + years(2000))
    
    tbl_store <- tbl(con_db, "_Reference156") %>%
        select("_IDRRef", link_parent_store = "_ParentIDRRef", marked = "_Marked",
               store_id = "_Code") %>% 
        mutate(marked = as.logical(marked))
    tbl_items <- tbl(con_db, "_Reference120") %>% 
        select("_IDRRef", item_id = "_Code")
    
    
    tbl_stock_movement <- tbl(con_db,"_AccumRg17702") %>% 
        select(date_movement = "_Period", active = "_Active",
               link_store = "_Fld17703RRef", link_item = "_Fld17704RRef",
               type_motion = "_RecordKind", item_motion_qty = "_Fld17708"
        ) %>%
        mutate(active = as.logical(active)) %>%
        filter(date_movement >= date_start,
               active == "TRUE") %>% 
        
        left_join(tbl_items, by = c(link_item = "_IDRRef")) %>%
        
        left_join(tbl_store, by = c(link_store = "_IDRRef")) %>% 
        left_join(select(tbl_store, "_IDRRef", parent_store = store_id),
                  by = c(link_parent_store = "_IDRRef")) %>%
        filter(!parent_store %in% c("000001096",         # Інші закриті
                                    "000001084")) %>%    # Обладнання
        
        mutate(date_movement = DATEADD(sql("month"),
                                       DATEDIFF(sql("month"), 0, date_movement),
                                       0)) %>%
        
        mutate(item_motion_qty = ifelse(type_motion == 0,
                                        item_motion_qty,
                                        -item_motion_qty)
        ) %>%
        
        group_by(date_movement, store_id, item_id) %>%
        summarise(store_item_moving_qty = sum(item_motion_qty, na.rm = TRUE)
        ) %>%
        ungroup() %>%
        filter(store_item_moving_qty != 0) %>%
        collect() %>% 
        mutate(date_movement = date_movement - years(2000))
    
    return(tbl_stock_movement)
    
}


get_current_inventory_batch <- function() {
    
    date_today <- as_datetime(today() + years(2000))
    
    tbl_store <- tbl(con_db, "_Reference156") %>%
        select("_IDRRef", link_parent_store = "_ParentIDRRef", marked = "_Marked",
               store_id = "_Code") %>% 
        mutate(marked = as.logical(marked))
    tbl_items <- tbl(con_db, "_Reference120") %>% 
        select("_IDRRef", item_id = "_Code")
    tbl_complete_set_doc <- tbl(con_db, "_Document298") %>%
        select("_IDRRef", date_compl_set_doc = "_Date_Time")
    tbl_receipt_doc <- tbl(con_db, "_Document360") %>%
        select("_IDRRef", date_receipt_doc = "_Date_Time")
    
    
    tbl_stock_balance <- tbl(con_db,"_AccumRgT17307") %>% 
        select(date_balance = "_Period", link_store = "_Fld17288RRef",
               link_item = "_Fld17287RRef", item_store_qty = "_Fld17295",
               item_store_sum = "_Fld17296", item_store_vat = "_Fld17297",
               link_receipt_doc = "_Fld17291_RRRef"
        ) %>% 
        filter(date_balance > date_today) %>% 
        left_join(tbl_items, by = c(link_item = "_IDRRef")) %>% 
        left_join(tbl_store, by = c(link_store = "_IDRRef")) %>% 
        left_join(select(tbl_store, "_IDRRef", parent_store = store_id),
                  by = c(link_parent_store = "_IDRRef")) %>%
        filter(!parent_store %in% c("000001096",         # Інші закриті
                                    "000001084")) %>%    # Обладнання
        
        mutate(item_store_vat = ifelse(
            item_store_vat < floor((item_store_sum / 5) * 10)/10,
            0,
            item_store_vat),
            item_store_cost = item_store_sum + item_store_vat,
        ) %>% 
        
        left_join(tbl_receipt_doc, by = c(link_receipt_doc = "_IDRRef")) %>%
        left_join(tbl_complete_set_doc, by = c(link_receipt_doc = "_IDRRef")) %>%
        
        mutate(date_receipt = ifelse(!is.na(date_receipt_doc),
                                     date_receipt_doc,
                                     date_compl_set_doc)) %>% 
        
        group_by(store_id, item_id, date_receipt) %>%
        summarise(item_store_qty = sum(item_store_qty),
                  item_store_sum= sum(item_store_cost)) %>%
        ungroup() %>%
        #filter(item_store_qty != 0) %>%
        
        collect() %>% 
        mutate(date_receipt = as_date(date_receipt) - years(2000))
    
    return(tbl_stock_balance)
    
}


get_inventory_movement_batch <- function(date_start) {
    
    date_start <- as_datetime(date_start) + years(2000)
    
    tbl_store <- tbl(con_db, "_Reference156") %>%
        select("_IDRRef", link_parent_store = "_ParentIDRRef", marked = "_Marked",
               store_id = "_Code") %>% 
        mutate(marked = as.logical(marked))
    tbl_items <- tbl(con_db, "_Reference120") %>% 
        select("_IDRRef", item_id = "_Code", "_ParentIDRRef")
    tbl_complete_set_doc <- tbl(con_db, "_Document298") %>%
        select("_IDRRef", date_compl_set_doc = "_Date_Time")
    tbl_receipt_doc <- tbl(con_db, "_Document360") %>%
        select("_IDRRef", date_receipt_doc = "_Date_Time")
    
    tbl_stock_movement <- tbl(con_db,"_AccumRg17286") %>% 
        select(date_movement = "_Period", active = "_Active",
               link_store = "_Fld17288RRef", link_item = "_Fld17287RRef",
               type_motion = "_RecordKind", item_motion_qty = "_Fld17295",
               item_store_sum ="_Fld17296", item_store_vat ="_Fld17297",
               link_receipt_doc = "_Fld17291_RRRef"
        ) %>%
        mutate(active = as.logical(active)) %>%
        filter(date_movement >= date_start,
               active == "TRUE") %>% 
        
        left_join(tbl_items, by = c(link_item = "_IDRRef")) %>%
        
        left_join(tbl_store, by = c(link_store = "_IDRRef")) %>% 
        left_join(select(tbl_store, "_IDRRef", parent_store = store_id),
                  by = c(link_parent_store = "_IDRRef")) %>%
        filter(!parent_store %in% c("000001096",         # Інші закриті
                                    "000001084"),        # Обладнання
        ) %>%
        
        left_join(tbl_receipt_doc, by = c(link_receipt_doc = "_IDRRef")) %>%
        left_join(tbl_complete_set_doc, by = c(link_receipt_doc = "_IDRRef")) %>%
        
        mutate(date_receipt = ifelse(!is.na(date_receipt_doc),
                                     date_receipt_doc,
                                     date_compl_set_doc)) %>% 
        
        mutate(date_movement = DATEADD(sql("month"),
                                       DATEDIFF(sql("month"), 0, date_movement),
                                       0)) %>%
        
        mutate(item_store_vat = ifelse(
            item_store_vat < floor((item_store_sum / 5) * 10)/10,
            0,
            item_store_vat),
            item_store_cost = item_store_sum + item_store_vat
        ) %>%
        
        mutate(item_motion_qty = ifelse(type_motion == 0,
                                        item_motion_qty,
                                        -item_motion_qty),
               item_motion_cost = ifelse(type_motion == 0,
                                         item_store_cost,
                                         -item_store_cost)
        ) %>% 
        
        group_by(date_movement, date_receipt, store_id, item_id) %>% 
        summarise(item_motion_qty = sum(item_motion_qty),
                  item_motion_cost = sum(item_motion_cost)) %>% 
        ungroup() %>% 
        filter(item_motion_qty != 0) %>%
        
        collect() %>%
        
        mutate(date_movement = date_movement - years(2000),
               date_receipt = as_date(date_receipt) - years(2000))
    
    return(tbl_stock_movement)
    
}


## Ціни номенклатури (Закупочні)
price_items_data <- function(date_start, price_type = "000000005") {
    
    date_start <- as_datetime(date_start) + years(2000)
    
    tbl_price_setting <- tbl(con_db,"_InfoRg16225") %>% 
        select(date_setting = "_Period",
               link_priceType = "_Fld16226RRef", link_item = "_Fld16227RRef",
               active = "_Active", item_price = "_Fld16230") %>% 
        mutate(active = as.logical(active)) %>% 
        filter(active == "TRUE")
    
    tbl_item <- tbl(con_db,"_Reference120") %>% 
        select("_IDRRef", item_id = "_Code")
    tbl_priceType <- tbl(con_db, "_Reference183") %>% 
        select("_IDRRef", priceType_id = "_Code")
    
    df_price_item <- tbl_price_setting %>%
        left_join(tbl_priceType, by = c(link_priceType = "_IDRRef")) %>% 
        filter(priceType_id == price_type) %>% 
        left_join(tbl_item,  by = c(link_item = "_IDRRef")) %>% 
        select(date_setting, item_id, item_price) %>%
        group_by(item_id) %>%
        filter(date_setting >= max(date_setting[date_setting < date_start])) %>%
        ungroup() %>%
        collect()
    
    return(df_price_item)
    
}


get_cash_balance <- function(date_finish) {
    
    date_finish <- as_datetime(date_finish + years(2000))
    
    tbl_cash <- tbl(con_db,"_AccumRg17031") %>% 
        select(date ="_Period", active ="_Active", type_moving ="_RecordKind",
               link_organiz = "_Fld17034RRef", link_account = "_Fld17033_RRRef",
               link_type_money = "_Fld17032RRef", moving_sum = "_Fld17035") %>% 
        mutate(active = as.logical(active)) %>% 
        filter(date < date_finish,
               active == "TRUE")
    
    tbl_organization <- tbl(con_db, "_Reference128") %>% 
        select("_IDRRef", organiz_id = "_Code")    
    tbl_type_money <- tbl(con_db, "_Enum480") %>% 
        select("_IDRRef", type_money_order = "_EnumOrder")
    tbl_bank_account <- tbl(con_db, "_Reference45") %>% 
        select("_IDRRef", account_id = "_Code", link_bank = "_Fld1385RRef",
               link_currency = "_Fld1388RRef")
    tbl_bank <- tbl(con_db, "_Reference44") %>% 
        select("_IDRRef", bank_name = "_Description")
    tbl_cashbox <- tbl(con_db, "_Reference88") %>% 
        select("_IDRRef", account_id = "_Code", link_currency = "_Fld1667RRef")
    tbl_currency <- tbl(con_db, "_Reference47") %>% 
        select("_IDRRef", currency_id = "_Code")
    tbl_exchange <- tbl(con_db, "_InfoRg14698") %>% 
        select(date_exchange ="_Period", link_currency = "_Fld14699RRef",
               currency_rate = "_Fld14700") %>% 
        filter(date_exchange == max(date_exchange[date_exchange < date_finish])) %>%
        left_join(tbl_currency,     by = c(link_currency = "_IDRRef")) %>%
        select(currency_id, currency_rate) %>% 
        collect()
    
    
    df_bank_account <- tbl_cash %>%
        left_join(tbl_organization, by = c(link_organiz = "_IDRRef")) %>%
        left_join(tbl_bank_account, by = c(link_account = "_IDRRef")) %>%
        filter(!is.na(account_id)) %>% 
        left_join(tbl_currency,     by = c(link_currency = "_IDRRef")) %>%
        mutate(moving_sum = ifelse(type_moving == 0,
                                   moving_sum,
                                   -moving_sum)) %>%
        group_by(organiz_id, account_id, currency_id) %>%
        summarise(acc_balance = sum(moving_sum)) %>%
        ungroup() %>% 
        select(organiz_id, account_id, acc_balance, currency_id) %>% 
        collect() %>% 
        filter(!near(acc_balance, 0)) %>% 
        mutate(type_acc = "безготівковий")
    
    df_cash_account <- tbl_cash %>%
        left_join(tbl_organization, by = c(link_organiz = "_IDRRef")) %>%
        left_join(tbl_cashbox,      by = c(link_account = "_IDRRef")) %>%
        filter(!is.na(account_id)) %>% 
        left_join(tbl_currency,     by = c(link_currency = "_IDRRef")) %>%
        mutate(moving_sum = ifelse(type_moving == 0,
                                   moving_sum,
                                   -moving_sum)) %>%
        group_by(account_id, currency_id) %>%
        summarise(acc_balance = sum(moving_sum)) %>%
        ungroup() %>% 
        select(#date,
            account_id, acc_balance, currency_id) %>% 
        collect() %>% 
        filter(!near(acc_balance, 0)) %>% 
        mutate(type_acc = "готівковий",
               organiz_id = "невідомий"
               ) %>% 
        relocate(organiz_id, .before = account_id)
    
    df_total <- df_bank_account %>% 
        bind_rows(df_cash_account) %>% 
        left_join(tbl_exchange) %>% 
        replace_na(list(currency_rate = 1)) %>% 
        mutate(sum = ifelse(currency_id == "980",
                                 acc_balance,
                                 acc_balance * currency_rate),
               subgroup = ifelse(organiz_id == "000000001" &
                                     type_acc == "безготівковий",
                                 "Корвет б/г",
                                 "Інші")) %>% 
        select(subgroup, sum)
        
    return(df_total)

}


get_money_on_way_not_korvet_start <- function() {
    
    account = "333"
    
    tbl_acc_credit <- tbl(con_db, "_AccRg855") %>% 
        select("_AccountCtRRef", date = "_Period", active = "_Active",
               link_organization = "_Fld856RRef", moving_sum = "_Fld859") %>% 
        mutate(active = as.logical(active)) %>% 
        filter(active == "TRUE")

    tbl_account <- tbl(con_db, "_Acc18") %>% 
        select("_IDRRef", acc_id = "_Code")
    tbl_organization <- tbl(con_db, "_Reference128") %>% 
        select("_IDRRef", organization_id = "_Code")
    
    acc_ct <- tbl_acc_credit %>% 
        left_join(tbl_account,      by = c("_AccountCtRRef" = "_IDRRef")) %>% 
        left_join(tbl_organization, by = c(link_organization = "_IDRRef")) %>%
        filter(!organization_id %in% "000000001",
               acc_id == account,
               as_date(date) == as_date("4022-01-04")
        ) %>% 
        summarise(ct_sum = sum(moving_sum)) %>% 
        collect() %>% pull
       
}


get_money_on_way_korvet <- function(date_balance) {
    
    account = "333"
    
    date_balance <- as_datetime(date_balance + years(2000))
    
    tbl_acc_credit <- tbl(con_db, "_AccRg855") %>% 
        select("_AccountCtRRef", date = "_Period", active = "_Active",
               link_organization = "_Fld856RRef", moving_sum = "_Fld859") %>% 
        mutate(active = as.logical(active)) %>% 
        filter(active == "TRUE",
               date < date_balance)
    tbl_acc_debet <- tbl(con_db, "_AccRg855") %>% 
        select("_AccountDtRRef", date = "_Period", active = "_Active",
               link_organization = "_Fld856RRef", moving_sum = "_Fld859") %>%
        mutate(active = as.logical(active)) %>%
        filter(date < date_balance,
               active == "TRUE")
    tbl_account <- tbl(con_db, "_Acc18") %>% 
        select("_IDRRef", acc_id = "_Code")
    tbl_organization <- tbl(con_db, "_Reference128") %>% 
        select("_IDRRef", organization_id = "_Code")
    
    acc_ct <- tbl_acc_credit %>% 
        left_join(tbl_account,      by = c("_AccountCtRRef" = "_IDRRef")) %>% 
        left_join(tbl_organization, by = c(link_organization = "_IDRRef")) %>%
        filter(organization_id == "000000001",
               acc_id == account) %>% 
        summarise(ct_sum = sum(moving_sum)) %>% 
        collect() %>% 
        pull
    
    acc_dt <- tbl_acc_debet %>% 
        left_join(tbl_account,      by = c("_AccountDtRRef" = "_IDRRef")) %>% 
        left_join(tbl_organization, by = c(link_organization = "_IDRRef")) %>%
        filter(organization_id == "000000001",
               acc_id == account) %>% 
        summarise(ct_sum = sum(moving_sum)) %>% 
        collect() %>% 
        pull()
    
    balance <- acc_dt - acc_ct
    
    return(balance)
    
}


get_money_on_way_not_korvet <- function(date_balance) {
    
    account = "333"
    
    date_balance <- as_datetime(date_balance + years(2000))
    
    tbl_acc_credit <- tbl(con_db, "_AccRg855") %>% 
        select("_AccountCtRRef", date = "_Period", active = "_Active",
               link_organization = "_Fld856RRef", moving_sum = "_Fld859") %>% 
        mutate(active = as.logical(active)) %>% 
        filter(active == "TRUE",
               date < date_balance#,
               #as_date(date) >= as_date("4022-01-01")
               )
    
    tbl_acc_debet <- tbl(con_db, "_AccRg855") %>% 
        select("_AccountDtRRef", date = "_Period", active = "_Active",
               link_organization = "_Fld856RRef", moving_sum = "_Fld859",
               link_doc = "_RecorderRRef") %>%
        mutate(active = as.logical(active)) %>%
        filter(date < date_balance,
               #as_date(date) >= as_date("4022-01-01"),
               active == "TRUE")
    
    tbl_account <- tbl(con_db, "_Acc18") %>% 
        select("_IDRRef", acc_id = "_Code")
    tbl_organization <- tbl(con_db, "_Reference128") %>% 
        select("_IDRRef", organization_id = "_Code")
    tbl_operation_doc <- tbl(con_db, "_Document319") %>%
        select("_IDRRef", date_operation_doc = "_Date_Time")
    
    acc_ct <- tbl_acc_credit %>% 
        left_join(tbl_account,      by = c("_AccountCtRRef" = "_IDRRef")) %>% 
        left_join(tbl_organization, by = c(link_organization = "_IDRRef")) %>%
        filter(organization_id != "000000001",
               acc_id == account) %>% 
        summarise(ct_sum = sum(moving_sum)) %>% 
        collect() %>% 
        pull()
    
    acc_dt <- tbl_acc_debet %>% 
        left_join(tbl_account,       by = c("_AccountDtRRef" = "_IDRRef")) %>% 
        left_join(tbl_organization,  by = c(link_organization = "_IDRRef")) %>%
        left_join(tbl_operation_doc, by = c(link_doc = "_IDRRef")) %>%
        filter(organization_id != "000000001",
               acc_id == account#,
               # !(!is.na(date_operation_doc) &
               #       as_date(date) == as_date("4022-01-04"))
               ) %>% 
        summarise(dt_sum = sum(moving_sum)) %>% 
        collect() %>% 
        pull()
    
    balance <- acc_dt - acc_ct
    
    return(balance)
    
}


## Данные по задолженности контрагентов на дату
get_partner_debt <- function(date_balance) {

    date_balance <- as_datetime(date_balance + years(2000))
    
    tbl_partner_debt_movement <- tbl(con_db,"_AccumRg16896") %>% 
        select(date_movement = "_Period", active = "_Active",
               link_organization = "_Fld16899RRef",
               link_partner = "_Fld16900RRef", link_contract = "_Fld16897RRef",
               type_moving = "_RecordKind", moving_sum = "_Fld16901") %>% 
        mutate(active = as.logical(active)) %>% 
        filter(date_movement < date_balance,
               active == "TRUE")
    
    tbl_partner <- tbl(con_db, "_Reference102") %>% 
        select("_IDRRef", partner_id = "_Code", "_ParentIDRRef", partner_name = "_Description")
    tbl_contract <- tbl(con_db, "_Reference78") %>% 
        select("_IDRRef", contract_name = "_Description",
               link_contract_type = "_Fld1600RRef",
               link_currency = "_Fld1583RRef")
    tbl_type_contract <- tbl(con_db, "_Enum484") %>% 
        select("_IDRRef", type_contract = "_EnumOrder")
    tbl_organization <- tbl(con_db, "_Reference128") %>% 
        select("_IDRRef", organization_id = "_Code")
    tbl_currency <- tbl(con_db, "_Reference47") %>% 
        select("_IDRRef", currency_id = "_Code")
    tbl_currency_rate <- tbl(con_db, "_InfoRg14698") %>% 
        select(date = "_Period", link_currency = "_Fld14699RRef",
               currency_rate = "_Fld14700")
    
    currency_rate_data <- tbl_currency_rate %>% 
        filter(date == max(date[date < date_balance])) %>% 
        left_join(tbl_currency, by = c(link_currency = "_IDRRef")) %>%
        select(currency_id, currency_rate)
    
    df_partner_motion <- tbl_partner_debt_movement %>%
        left_join(tbl_partner,       by = c(link_partner = "_IDRRef")) %>% 
        left_join(tbl_contract,      by = c(link_contract = "_IDRRef")) %>% 
        left_join(tbl_type_contract, by = c(link_contract_type = "_IDRRef")) %>%
        left_join(tbl_organization,  by = c(link_organization = "_IDRRef")) %>%
        left_join(tbl_currency, by = c(link_currency = "_IDRRef")) %>%
        mutate(moving_sum = ifelse(type_moving == 0,
                                   moving_sum,
                                   -moving_sum)) %>% 
        group_by(organization_id, partner_id, contract_name, type_contract,
                 currency_id) %>% 
        summarise(balance = sum(moving_sum)) %>% 
        ungroup() %>% 
        filter(balance != 0,
               type_contract %in% c(0,1)) %>% 
        left_join(currency_rate_data, by = "currency_id") %>% 
        replace_na(list(currency_rate = 1)) %>% 
        mutate(balance = balance * currency_rate)
    
    customers_data <- df_partner_motion %>%
        left_join(select(tbl_partner, partner_id, "_ParentIDRRef"),
                  by = "partner_id") %>%
        left_join(select(tbl_partner, "_IDRRef", folder_partner = partner_id,
                         folder_name = partner_name),
                  by = c("_ParentIDRRef" = "_IDRRef")) %>% 
        filter(type_contract == 1,
               !folder_partner %in% c("000423140",    # РЕГИОНАЛЬНЫЕ МАГАЗИНЫ
                                      "001004385"     # КІНЦЕВИЙ ПОКУПЕЦЬ
                                      )
        ) %>% 
        mutate(organization = ifelse(organization_id == "000000001",
                                     "Корвет",
                                     "ПП"),
               type_debt = ifelse(balance < 0,
                                  "credit",
                                  "debit")) %>% 

        group_by(organization, type_debt) %>%
        summarise(balance_sum = round(sum(balance))) %>%
        collect() %>% 
        mutate(balance_sum = ifelse(type_debt == "debit",
                                    balance_sum,
                                    -balance_sum),
               debt_owner = "customer")
    
    suppliers_data <- df_partner_motion %>%
        left_join(select(tbl_partner, partner_id, "_ParentIDRRef"),
                  by = "partner_id") %>%
        left_join(select(tbl_partner, "_IDRRef", folder_partner = partner_id,
                         folder_name = partner_name),
                  by = c("_ParentIDRRef" = "_IDRRef")) %>% 
        filter(type_contract == 0,
               !contract_name %like% "%685%",
               !folder_partner %in% "000423140"    # РЕГИОНАЛЬНЫЕ МАГАЗИНЫ
        ) %>% 
        select(organization_id, partner_id, contract_name, balance) %>% 
        # mutate(organization = ifelse(organization_id == "000000001",
        #                              "Корвет",
        #                              "ПП"),
        #        type_debt = ifelse(balance < 0,
        #                           "credit",
        #                           "debit")) %>% 
        # group_by(organization, type_debt) %>%
        # summarise(balance_sum = round(sum(balance))) %>%
        
        collect() %>% 
        mutate(balance_sum = ifelse(type_debt == "debit",
                                    balance_sum,
                                    -balance_sum),
               debt_owner = "supplier")
    
    service_suppliers_data <- df_partner_motion %>%
        left_join(select(tbl_partner, partner_id, "_ParentIDRRef"),
                  by = "partner_id") %>%
        left_join(select(tbl_partner, "_IDRRef", folder_partner = partner_id,
                         folder_name = partner_name),
                  by = c("_ParentIDRRef" = "_IDRRef")) %>% 
        filter(type_contract == 0,
               contract_name %like% "%685%",
               !folder_partner %in% "000423140"    # РЕГИОНАЛЬНЫЕ МАГАЗИНЫ
        ) %>% 
        mutate(organization = ifelse(organization_id == "000000001",
                                     "Корвет",
                                     "ПП"),
               type_debt = ifelse(balance < 0,
                                  "credit",
                                  "debit")) %>% 
        group_by(organization, type_debt) %>%
        summarise(balance_sum = round(sum(balance))) %>%
        collect() %>% 
        mutate(balance_sum = ifelse(type_debt == "debit",
                                    balance_sum,
                                    -balance_sum),
               debt_owner = "service_supplier")
    
    result <- bind_rows(customers_data, suppliers_data, service_suppliers_data)
    
    return(result)
    
    dbDisconnect(con)
}


## Данные по задолженности подотчетников
get_accountable_person_data <- function(date_balance) {
    
    date_balance <- as_datetime(date_balance + years(2000))
    
    tbl_person_debt_movement <- tbl(con_db,"_AccumRg16917") %>% 
        select(date_movement = "_Period", active = "_Active",
               link_person = "_Fld16918RRef", link_currency = "_Fld16919RRef",
               type_moving = "_RecordKind", moving_sum = "_Fld16922") %>% 
        mutate(active = as.logical(active)) %>% 
        filter(date_movement < date_balance,
               active == "TRUE")
    
    tbl_person <- tbl(con_db, "_Reference191") %>% 
        select("_IDRRef", person_id = "_Code")
    tbl_currency <- tbl(con_db, "_Reference47") %>% 
        select("_IDRRef", currency_id = "_Code")
    tbl_currency_rate <- tbl(con_db, "_InfoRg14698") %>% 
        select(date = "_Period", link_currency = "_Fld14699RRef",
               currency_rate = "_Fld14700")
    
    currency_rate_data <- tbl_currency_rate %>% 
        filter(date == max(date[date < date_balance])) %>% 
        left_join(tbl_currency, by = c(link_currency = "_IDRRef")) %>%
        select(currency_id, currency_rate)
    
    result <- tbl_person_debt_movement %>%
        left_join(tbl_person,   by = c(link_person = "_IDRRef")) %>% 
        left_join(tbl_currency, by = c(link_currency = "_IDRRef")) %>%
        mutate(moving_sum = ifelse(type_moving == 0,
                                   moving_sum,
                                   -moving_sum)) %>% 
        group_by(person_id, currency_id) %>% 
        summarise(balance = sum(moving_sum)) %>% 
        ungroup() %>% 
        filter(balance != 0) %>% 
        left_join(currency_rate_data, by = "currency_id") %>% 
        replace_na(list(currency_rate = 1)) %>% 
        mutate(balance_grn = balance * currency_rate) %>% 
        collect() %>% 
        summarise(round(sum(balance_grn))) %>% 
        pull
    

    return(result)
    
    dbDisconnect(con)
}


## Данные по задолженности по налогам
get_payroll_taxes <- function(date_balance) {
    
    date_balance <- as_datetime(date_balance + years(2000))
    last_month <- as_date(date_balance) - months(1)
    
    tbl_payroll_taxes_worker <- tbl(con_db,"_AccumRg16860") %>% 
        select(date_movement = "_Period", date_settlement = "_Fld16864",
               active = "_Active",
               link_employee = "_Fld16861_RRRef",
               link_organization = "_Fld16862RRef",
               type_moving = "_RecordKind",
               tax_sum = "_Fld16868") %>% 
        mutate(active = as.logical(active)) %>% 
        filter(date_movement < date_balance,
               active == "TRUE")
    
    tbl_payroll_taxes_firm <- tbl(con_db,"_AccumRg16830") %>% 
        select(date_movement = "_Period", date_settlement = "_Fld16832",
               active = "_Active",
               link_doc = "_RecorderRRef",
               link_employee = "_Fld16841RRef",
               link_organization = "_Fld16831RRef",
               type_moving = "_RecordKind",
               tax_sum = "_Fld16838") %>% 
        mutate(active = as.logical(active)) %>% 
        filter(date_movement < date_balance,
               active == "TRUE")
    
    tbl_payment_doc <- tbl(con_db, "_Document352") %>%
        select("_IDRRef", date_payment_doc = "_Date_Time")
    
    tbl_employee <- tbl(con_db, "_Reference159") %>% 
        select("_IDRRef", employee_id = "_Code")
    tbl_organization <- tbl(con_db, "_Reference128") %>% 
        select("_IDRRef", organization_id = "_Code")

    debt_workers <- tbl_payroll_taxes_worker %>%
        filter(as_date(date_settlement) == last_month) %>% 
        left_join(tbl_employee,     by = c(link_employee = "_IDRRef")) %>% 
        left_join(tbl_organization, by = c(link_organization = "_IDRRef")) %>%
        mutate(tax_sum = ifelse(type_moving == 0,
                                tax_sum,
                                -tax_sum),
               organization = ifelse(organization_id == "000000001",
                                     "Корвет",
                                     "ПП")) %>% 
        group_by(organization) %>%
        summarise(tax_sum = sum(tax_sum, na.rm = TRUE)) %>%
        # select(date_movement, date_settlement, organization, employee_id, tax_sum) %>% 
        collect() %>% 
        mutate(type = "debt")
    
    debt_firm <- tbl_payroll_taxes_firm %>%
        left_join(tbl_employee,     by = c(link_employee = "_IDRRef")) %>% 
        left_join(tbl_organization, by = c(link_organization = "_IDRRef")) %>%
        left_join(tbl_payment_doc,  by = c(link_doc = "_IDRRef")) %>%
        filter(as_date(date_settlement) == last_month,
               is.na(date_payment_doc) | date_payment_doc < date_balance
               ) %>% 
        mutate(tax_sum = ifelse(type_moving == 0,
                                tax_sum,
                                -tax_sum),
               organization = ifelse(organization_id == "000000001",
                                     "Корвет",
                                     "ПП")
        ) %>% 
        group_by(organization) %>%
        summarise(tax_sum = sum(tax_sum, na.rm = TRUE)) %>%
        # select(date_movement, date_settlement, date_payment_doc, organization_id, employee_id, tax_sum) %>% 
        collect() %>% 
        mutate(type = "debt")
    
    # Виплачені відпускні за майбутній період
    prepaid_workers <- tbl_payroll_taxes_worker %>%
        filter(as_date(date_settlement) > last_month) %>% 
        left_join(tbl_employee,     by = c(link_employee = "_IDRRef")) %>% 
        left_join(tbl_organization, by = c(link_organization = "_IDRRef")) %>%
        mutate(tax_sum = ifelse(type_moving == 0,
                                tax_sum,
                                -tax_sum),
               organization = ifelse(organization_id == "000000001",
                                     "Корвет",
                                     "ПП")) %>% 
        group_by(organization) %>%
        summarise(tax_sum = -sum(tax_sum, na.rm = TRUE)) %>%
        # select(date_movement, date_settlement, organization, employee_id, tax_sum) %>% 
        collect() %>% 
        mutate(type = "prepaid")
    
    prepaid_firm <- tbl_payroll_taxes_firm %>%
        filter(as_date(date_settlement) > last_month) %>% 
        left_join(tbl_employee,     by = c(link_employee = "_IDRRef")) %>% 
        left_join(tbl_organization, by = c(link_organization = "_IDRRef")) %>%
        mutate(tax_sum = ifelse(type_moving == 0,
                                tax_sum,
                                -tax_sum),
               organization = ifelse(organization_id == "000000001",
                                     "Корвет",
                                     "ПП")
               ) %>% 
        group_by(organization) %>%
        summarise(tax_sum = -sum(tax_sum, na.rm = TRUE)) %>%
        # select(date_movement, date_settlement, organization_id, employee_id, tax_sum) %>% 
        collect() %>% 
        mutate(type = "prepaid")
    
    result <- bind_rows(debt_workers, debt_firm, prepaid_workers, prepaid_firm) %>% 
        group_by(organization, type) %>% 
        summarise(tax_sum = sum(tax_sum))
    
    return(result)
    
    dbDisconnect(con)
}


## Данные по задолженности по зарплате
get_payroll_debt <- function(date_balance) {
    
    date_balance <- as_datetime(date_balance + years(2000))
    last_month <- as_date(date_balance) - months(1)
    
    tbl_payroll <- tbl(con_db,"_AccumRg16926") %>% 
        select(date_movement = "_Period", date_settlement = "_Fld16929",
               active = "_Active",
               link_employee = "_Fld16927RRef",
               link_type_operation = "_Fld16936RRef",
               type_moving = "_RecordKind",
               operation_sum = "_Fld16933",
               management_sum = "_Fld16934") %>% 
        mutate(active = as.logical(active)) %>% 
        filter(date_movement < date_balance,
               active == "TRUE")
    

    tbl_employee <- tbl(con_db, "_Reference159") %>% 
        select("_IDRRef", employee_id = "_Code")
    # tbl_organization <- tbl(con_db, "_Reference128") %>% 
    #     select("_IDRRef", organization_id = "_Code")
    tbl_type_operation <- tbl(con_db, "_Enum618") %>% 
        select("_IDRRef", operation_id = "_EnumOrder")
    

    debt_payroll <- tbl_payroll %>%
        left_join(tbl_employee,       by = c(link_employee = "_IDRRef")) %>% 
        #left_join(tbl_organization,   by = c(link_organization = "_IDRRef")) %>%
        left_join(tbl_type_operation, by = c(link_type_operation = "_IDRRef"))%>%
        filter(as_date(date_settlement) == last_month,
               (!is.na(management_sum) & operation_id == 0 &      # Начисления
                   management_sum != 0) |
                   (!is.na(operation_sum) & operation_id == 4 &    # Выплаты
                        operation_sum != 0)
        ) %>% 
        mutate(payroll_sum = case_when(
                   type_moving == 0 & management_sum != 0  ~ management_sum,
                   type_moving == 1 & management_sum != 0  ~ -management_sum,
                   type_moving == 1 & operation_sum != 0   ~ -operation_sum
                   ),
               # organization = ifelse(organization_id == "000000001",
               #                       "Корвет",
               #                       "ПП")
        ) %>% 
        # group_by(organization) %>%
        summarise(payroll_sum = sum(payroll_sum, na.rm = TRUE)) %>%
        # select(date_movement, date_settlement, organization_id, employee_id, type_moving, management_sum, operation_sum, payroll_sum) %>% 
        collect() %>% 
        mutate(type = "debt")
    
    # Виплачені відпускні за майбутній період
    prepaid_payroll <- tbl_payroll %>%
        left_join(tbl_type_operation, by = c(link_type_operation = "_IDRRef"))%>%
        filter(as_date(date_settlement) > last_month,
               (!is.na(management_sum) & operation_id == 0 &      # Начисления
                    management_sum != 0) |
                   (!is.na(operation_sum) & operation_id == 4 &    # Выплаты
                        operation_sum != 0)) %>% 
        left_join(tbl_employee,     by = c(link_employee = "_IDRRef")) %>% 
        
        # left_join(tbl_organization, by = c(link_organization = "_IDRRef")) %>%
        mutate(payroll_sum = case_when(
            type_moving == 0 & management_sum != 0  ~ management_sum,
            type_moving == 1 & management_sum != 0  ~ -management_sum,
            type_moving == 1 & operation_sum != 0   ~ -operation_sum
        )
        ) %>% 
        # group_by(organization) %>%
        summarise(payroll_sum = -sum(payroll_sum, na.rm = TRUE)) %>%
        # select(date_movement, date_settlement, employee_id, type_moving, management_sum, operation_sum, payroll_sum) %>% 
        collect() %>% 
        mutate(type = "prepaid")
    
    result <- bind_rows(debt_payroll, prepaid_payroll) %>% 
        replace_na(list(payroll_sum = 0))
    
    return(result)
    
    dbDisconnect(con)
}


## Кредитовий рух по певному рахунку Корвет за період
account_credit_moving <- function(date_start, date_finish, account = "6413",
                            organization = "000000001") {
    
    date_start <- as_datetime(date_start + years(2000))
    date_finish <- as_datetime(date_finish + years(2000))
    
    tbl_acc_credit <- tbl(con_db, "_AccRg855") %>% 
        select("_AccountCtRRef", date = "_Period", active = "_Active",
               link_organization = "_Fld856RRef", moving_sum = "_Fld859") %>% 
        mutate(active = as.logical(active)) %>% 
        filter(date < date_finish,
               date >= date_start,
               active == "TRUE")

    tbl_account <- tbl(con_db, "_Acc18") %>% 
        select("_IDRRef", acc_id = "_Code")
    tbl_organization <- tbl(con_db, "_Reference128") %>% 
        select("_IDRRef", organization_id = "_Code")
    
    acc_ct <- tbl_acc_credit %>% 
        left_join(tbl_account,      by = c("_AccountCtRRef" = "_IDRRef")) %>% 
        left_join(tbl_organization, by = c(link_organization = "_IDRRef")) %>%
        filter(organization_id == organization,
               acc_id == account) %>% 
        summarise(ct_sum = sum(moving_sum)) %>% 
        collect() %>% 
        pull

}
