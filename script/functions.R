get_sale_data <- function(date_start, date_finish) {
    
    cost_df <- get_cost_data(date_start,
                             date_finish) %>% 
        mutate(date = floor_date(as_date(date) - years(2000),
                                 unit = "quarter")) %>% 
        group_by(date) %>% 
        summarise(sum = sum(cost_sum)) %>% 
        mutate(project = "Собівартість")
    
    other_incom_df <- get_other_income(date_start,
                                         date_finish) %>% 
        mutate(date = floor_date(as_date(date) - years(2000),
                                 unit = "quarter")) %>% 
        group_by(date, project) %>% 
        summarise(sum = sum(sale_sum)) %>% 
        ungroup()
    
    sale_df <- get_sale_total(date_start,
                               date_finish) %>% 
        mutate(date = floor_date(as_date(date) - years(2000),
                                 unit = "quarter")) %>% 
        group_by(date, project) %>% 
        summarise(sum = sum(sale_sum)) %>% 
        bind_rows(other_incom_df) %>% 
        bind_rows(cost_df) %>% 
        filter(!project %in% "Курсові різниці") %>% 
        mutate(group = case_when(
            project %in% c("B2B", "B2C")  ~ paste0("Дохід від ", project),
            project %in% c("Бонуси постач.", "Списана кредит.заборг.", "Інше",
                           "deductions")  ~ "Інші доходи", # "Інші операц. доходи",
            project %in% c("Відсотки отримані",
                           "rent")        ~ "Інші доходи",
            .default = project
        )) %>% 
        select(-project) %>% 
        group_by(date, group) %>% 
        summarise(sum = sum(sum)) %>% 
        ungroup() %>% 
        arrange(date, group, .locale = "uk_UA") %>% 
        mutate(date = paste0(year(date), ":", quarter(date), "кв")) %>% 
        pivot_wider(names_from = "group", values_from = "sum")

    return(sale_df)
}


get_expenses <- function(date_start, date_finish) {
    
    vat_saldo_start <- account_balance(date_start)
    
    vat_data <- account_balance_motion(date_start,
                                       date_finish) %>% 
        add_row(date = date_start - months(1),
                saldo = vat_saldo_start) %>% 
        arrange(date) %>% 
        mutate(vat_sum = cumsum(saldo)) %>% 
        slice(-1) %>% 
        select(date, vat_sum) %>% 
        mutate(exp_sum = ifelse(vat_sum < 0,
                                -round(vat_sum),
                                0),
               expenses_group = "Податки(ПДВ та прибуток)",
               expenses_subgroup = "ПДВ",
               date = date - years(2000))
    

    expenses_df <- get_expenses_data(date_start,
                                     date_finish) %>%
        mutate(
            expenses_group = case_when(
                parent_exp_id %in% "100000198" |
                    expItems_id %in% "100000308"  ~ "З/п з податками",
                parent_exp_id %in% c("100000196",
                                     "100000201",
                                     "100000493") ~ "Оренда з комунальними",
                parent_exp_id %in% c("100000424",
                                     "100000425",
                                     "100000426",
                                     "100000427",
                                     "100000429",
                                     "100000446") ~ "Маркетинг",
                parent_exp_id %in% c("100000209",
                                     "100000212",
                                     "100000217") ~ "Автовитрати",
                parent_exp_id %in% "100000227"    ~ "Податки(ПДВ та прибуток)",
                .default =                          "Інші"),
            expenses_subgroup = case_when(
                expItems_id %in% "000000005"      ~ "Оклади",
                expItems_id %in% "100000376"      ~ "Премії",
                expItems_id %in% c("000000006",
                                   "100000308")   ~ "Податки на з/п",
                parent_exp_id %in% "100000196"    ~ "Оренда",
                parent_exp_id %in% c("100000201",
                                     "100000493") ~ "Комунальні посл.",
                parent_exp_id %in% "100000424"    ~ "Прямий маркетинг",
                parent_exp_id %in% c("100000425",
                                     "100000426",
                                     "100000427",
                                     "100000429",
                                     "100000446") ~ "Інші",
                parent_exp_id %in% c("100000209",
                                     "100000217") ~ "Утримання авто", 
                parent_exp_id %in% "100000212"    ~ "Паливо",
                parent_exp_id %in% "100000227"    ~ "Податок на прибуток",
                parent_exp_id %in% "100000232"    ~ "Спонсорська допомога",
                parent_exp_id %in% c("100000231",
                                     "100000230") ~ "Капітальні інвестиції",
                parent_exp_id %in% "100000403"    ~ "Курсові різниці",
               # expItems_id %in% "100000459"      ~ "Штраф від постач.",
               .default =                          "Інші витрати"
               )
        ) %>%
        mutate(date = date - years(2000)) %>% 
        select(date, expenses_group, expenses_subgroup, exp_sum) %>% 
        bind_rows(vat_data) %>% 
        group_by(date, expenses_group, expenses_subgroup) %>% 
        summarise(exp_sum = sum(exp_sum, na.rm = TRUE)) %>% 
        ungroup()
    
    return(expenses_df)
    
}


remove_unnecessary_items <- function(data) {
    
    data <- data %>% left_join(select(ref_items, item_id, main_parent),
                               by = "item_id") %>% 
        filter(!main_parent %in% c("000500558",           # Бухгалтерские
                                   "000300000",           # Торгів.обладнання
                                   "000400010")           # Рекламні материали
        ) %>% 
        select(-main_parent)
    
}


get_inventory_data <- function(date_start, date_finish) {
    
    calculate_date_inventory <- function(date_start) {
        
        inventory_data <- inventory_movement_data %>% 
            filter(date_movement >= date_start) %>% 
            group_by(store_id, item_id) %>% 
            summarise(store_item_moving_qty = sum(store_item_moving_qty))%>% 
            ungroup() %>% 
            full_join(current_inventory, by = c("store_id", "item_id")) %>% 
            replace_na(list(item_store_qty = 0,
                            store_item_moving_qty = 0)
            ) %>%
            mutate(balance_qty = item_store_qty - store_item_moving_qty
            ) %>% 
            filter(balance_qty != 0) %>% 
            select(-store_item_moving_qty, -item_store_qty)
        
    }
    
    
    calculate_date_inventory_batch <- function(date_start) {
        
        inventory_data <- inventory_movement_batch_data %>% 
            filter(date_movement >= date_start) %>% 
            select(-date_movement) %>% 
            rename(item_store_qty = item_motion_qty,
                   item_store_sum = item_motion_cost) %>% 
            mutate(item_store_qty = -item_store_qty,
                   item_store_sum = -item_store_sum) %>% 
            bind_rows(current_inventory_batch) %>% 
            group_by(store_id, item_id, date_receipt) %>%
            summarise(balance_batch_qty = sum(item_store_qty),
                      balance_batch_sum = sum(item_store_sum))%>%
            ungroup() %>%
            filter(!near(balance_batch_qty, 0),
                   !(balance_batch_qty < 0 & balance_batch_sum > 0))
    }
    
    calculate_inventory_sum <- function(date_start, data, data_batch) {
        
        joined_data <- left_join(data, data_batch,
                                 by = c("store_id", "item_id"),
                                 multiple = "all") %>%
            arrange(store_id, item_id, desc(date_receipt)) %>% 
            group_by(store_id, item_id) %>% 
            mutate(cum_balance_batch_qty = cumsum(balance_batch_qty),
                   cum_balance_batch_sum = cumsum(balance_batch_sum))
        
        equal_data <- joined_data %>% 
            filter(max(cum_balance_batch_qty) == balance_qty) %>% 
            filter(cum_balance_batch_qty == balance_qty) %>%
            slice(1) %>% 
            transmute(balance_sum = cum_balance_batch_sum) %>% 
            ungroup()
        
        batch_more_data <- joined_data %>% 
            filter(cum_balance_batch_qty > balance_qty) %>% 
            slice(1) %>% 
            transmute(balance_sum = ifelse(
                balance_batch_qty == cum_balance_batch_qty,
                round(balance_batch_sum / balance_batch_qty * balance_qty, 2),
                round(cum_balance_batch_sum - balance_batch_sum +
                          balance_batch_sum / balance_batch_qty *
                          (balance_batch_qty - (cum_balance_batch_qty - balance_qty)
                          ),
                      2))) %>% 
            ungroup()
        
        batch_less_data <- joined_data %>% 
            filter(max(cum_balance_batch_qty) < balance_qty) %>% 
            slice_tail(n = 1) %>% 
            transmute(balance_sum = ifelse(
                balance_batch_qty == cum_balance_batch_qty,
                round(balance_batch_sum / balance_batch_qty * balance_qty, 2),
                round(cum_balance_batch_sum +
                          balance_batch_sum / balance_batch_qty *
                          (balance_qty - cum_balance_batch_qty),
                      2))) %>% 
            ungroup()
        
        without_batch <- joined_data %>% 
            filter(is.na(cum_balance_batch_qty)) %>% 
            select(store_id:balance_qty) %>%  
            ungroup() %>% 
            mutate(date = date_start) %>% 
            left_join(prices_df,
                      join_by(item_id, closest(date > date_setting))) %>% 
            mutate(balance_sum = balance_qty * item_price) %>% 
            select(store_id, item_id, balance_sum)
        
        result <- bind_rows(equal_data, batch_more_data, batch_less_data,
                            without_batch) %>% 
            summarise(inventory_sum = sum(balance_sum, na.rm = TRUE)) %>% 
            pull()
        
    }
    
    
    current_inventory <- get_current_inventory() %>% 
        remove_unnecessary_items()
    
    inventory_movement_data <- get_inventory_movement(date_start) %>% 
        remove_unnecessary_items()
    
    current_inventory_batch <- get_current_inventory_batch() %>% 
        remove_unnecessary_items()
    
    inventory_movement_batch_data <- get_inventory_movement_batch(
        date_start) %>% 
        remove_unnecessary_items()
    
    prices_df <- price_items_data(date_start) %>% 
        mutate(date_setting = as_date(date_setting) - years(2000)) %>% 
        group_by(item_id, date_setting) %>% 
        slice_tail(n=1) %>% 
        ungroup()
    
    
    inventory_data <- tibble(
        date = seq.Date(date_start, date_finish, by = "quarter")
    ) %>%
        mutate(data = map(date, calculate_date_inventory)) %>% 
        mutate(data_batch = map(date, calculate_date_inventory_batch)) %>% 
        mutate(inventory_sum = pmap_dbl(list(date, data, data_batch),
                               calculate_inventory_sum)) %>% 
        select(date, sum = inventory_sum)
    
    return(inventory_data)
}


get_cash_data <- function(date_start, date_finish) {
    
    cash_data <- tibble(
        date = seq.Date(date_start, date_finish, by = "quarter")
    ) %>%
        mutate(data =  map(date, get_cash_balance)) %>% 
        unnest(cols = c(data))
    
    money_on_way_korvet <- tibble(
        date = seq.Date(date_start, date_finish, by = "quarter")
    ) %>%
        mutate(sum = map_dbl(date, get_money_on_way_korvet),
               subgroup = "Корвет б/г" )
        
    
    # money_on_way_not_korvet_start <- get_money_on_way_not_korvet_start()
    
    money_on_way_not_korvet <- tibble(
        date = seq.Date(date_start, date_finish, by = "quarter")
    ) %>%
        mutate(sum = map_dbl(date, get_money_on_way_not_korvet)) %>% 
        # add_row(date = date_start,
        #         sum = money_on_way_not_korvet_start,
        #         .before = 1) %>% 
        # mutate(sum = ifelse(date == as_date("2022-01-01"),
        #                     sum,
        #                     sum + money_on_way_not_korvet_start),
        #        subgroup = "Інші") %>% 
        mutate(sum = ifelse(date <= as_date("2023-01-01"),
                            sum + 51497 + 4547,
                            sum),
               subgroup = "Інші")
        #select(date, sum, subgroup)
    
    result <- bind_rows(cash_data, money_on_way_korvet, money_on_way_not_korvet) %>% 
        group_by(date, subgroup) %>% 
        summarise(sum = sum(sum)) %>% 
        ungroup()
    
}


get_debt_data <- function(date_start, date_finish) {
    
    debt_data <- tibble(
        date = seq.Date(date_start, date_finish, by = "quarter")
    ) %>%
        mutate(data =  map(date, get_partner_debt)) %>% 
        unnest(cols = c(data))
    
}


get_accountable_person <- function(date_start, date_finish) {
    
    debt_data <- tibble(
        date = seq.Date(date_start, date_finish, by = "quarter")
    ) %>%
        mutate(sum =  map_dbl(date, get_accountable_person_data),
               group = "Дебіторська заборгованість",
               subgroup = "Аванси інші")
    
}  


get_payroll_taxes_data <- function(date_start, date_finish) {
    
    payroll_taxes <- tibble(
        date = seq.Date(date_start, date_finish, by = "quarter")
    ) %>%
        mutate(data =  map(date, get_payroll_taxes)) %>% 
        unnest(cols = c(data))
}


get_payroll_debt_data <- function(date_start, date_finish) {
    
    payroll_taxes <- tibble(
        date = seq.Date(date_start, date_finish, by = "quarter")
    ) %>%
        mutate(data =  map(date, get_payroll_debt)) %>% 
        unnest(cols = c(data))
}


get_taxes_debt_data <- function(date_start, date_finish) {
    
    vat_tax <- tibble(
        date = seq.Date(date_start, date_finish, by = "quarter")
    ) %>%
        mutate(sum =  map_dbl(date, account_balance),
               type_tax = "vat") %>% 
        mutate(sum = ifelse(sum < 0,
                            -round(sum),
                            0))
    
    profit_tax <- tibble(
        date = seq.Date(date_start, date_finish, by = "quarter")
    ) %>%
        mutate(sum =  map2_dbl(date - months(3), date,
                                   account_credit_moving),
               type_tax = "profit")
    
    property_tax <- tibble(
        date = seq.Date(date_start, date_finish, by = "quarter")
    ) %>%
        mutate(sum =  pmap_dbl(list(date, "6415", "000000066"),
                                   account_balance),
               type_tax = "property") %>% 
        mutate(sum = ifelse(sum < 0,
                            -round(sum),
                            0))
    
    single_tax <- tibble(
        date = seq.Date(date_start, date_finish, by = "quarter")
    ) %>%
        mutate(sum =  pmap_dbl(list(date, "6414", "000000066"),
                                   account_balance),
               type_tax = "single") %>% 
        mutate(sum = ifelse(sum < 0,
                            -round(sum),
                            0))
    
    result <- bind_rows(vat_tax, profit_tax, property_tax, single_tax)
}


get_pnl <- function(sale_data, expenses_data) {
    
    expenses_df <- expenses_data %>% 
        group_by(date) %>% 
        summarise("Витрати" = sum(exp_sum)) %>% 
        mutate(date = paste0(year(date), ":", quarter(date), "кв"))
    
    
    sale_df <- sale_data %>% 
        left_join(expenses_df, by = "date") %>% 
        mutate(across(where(is.numeric), ~ round(.x/1000, 1))) %>% 
        mutate("Дохід від продажів" = `Дохід від B2B` + `Дохід від B2C`,
               "Вал.прибуток" = `Дохід від продажів` - `Собівартість`,
               "Рентабельність продажів" = round(`Вал.прибуток` /
                                                     `Дохід від продажів`, 3),
               "Чистий прибуток" = `Вал.прибуток` - `Витрати` + `Інші доходи`,
               "Рентабельність чист.прибутку" = round(`Чистий прибуток` /
                                                             `Дохід від продажів`, 3)) %>% 
        
        pivot_longer(cols = 2:ncol(.), names_to = "group") %>% 
        pivot_wider(names_from = "date", values_from = "value") %>% 
        
        mutate(group = factor(group, levels = c("Дохід від B2C",
                                                "Дохід від B2B",
                                                "Дохід від продажів",
                                                "Собівартість",
                                                "Вал.прибуток",
                                                "Рентабельність продажів",
                                                "Витрати",
                                                "Інші доходи",
                                                "Чистий прибуток",
                                                "Рентабельність чист.прибутку"))) %>% 
        arrange(group) %>% 
        mutate("TP" = round(.[[6]] / .[[2]], 2)) %>% 
        group_by(index = row_number()) %>% 
        mutate("Частка" = round(.[[6]][[index]] / .[[6]][[3]], 3)) %>% 
        ungroup()
    
}


get_expenses_detailed <- function(expenses_data) {
    
    last_total_expenses <- sum(expenses_data$exp_sum[expenses_data$date == date_finish - months(3)])/1000
    
    expenses_detailed <- expenses_data %>% 
        complete(date, nesting(expenses_group, expenses_subgroup),
                 fill = list(exp_sum = 0)) %>% 
        mutate(across(where(is.numeric), \(x) round(x/1000, 1))) %>%  
        group_by(expenses_group, expenses_subgroup) %>% 
        summarise(expenses_sum = list(exp_sum)) %>% 
        
        mutate(expenses_group = factor(expenses_group,
                                       levels = c("З/п з податками",
                                                  "Оренда з комунальними",
                                                  "Маркетинг",
                                                  "Автовитрати",
                                                  "Податки(ПДВ та прибуток)",
                                                  "Інші"))) %>% 
        arrange(expenses_group) %>% 
        rowwise() %>% 
        mutate("change" = round(expenses_sum[[5]] / expenses_sum[[1]], 2)) %>% 
        mutate("prop" = round(expenses_sum[[5]] / last_total_expenses, 3))
    
}


get_balance_tbl <- function(cash_data, accountable_person_data,
                                  payroll_taxes_data, payroll_debt_data,
                                  taxes_debt_data, inventory_data) {
    
    balance_tbl <- cash_data %>% 
        mutate(group = "Грошові кошти") %>% 
        bind_rows(debt_data %>% 
                      group_by(date, debt_owner, type_debt) %>% 
                      summarise(sum = sum(balance_sum)) %>% 
                      ungroup() %>% 
                      mutate(
                          group = case_when(
                              type_debt == "debit"   ~ "Дебіторська заборгованість",
                              type_debt == "credit"  ~ "Кредиторська заборгованість"),
                          subgroup = case_when(
                              debt_owner == "customer" & 
                                  type_debt == "debit" ~ "Заборгованість покупців",
                              debt_owner == "customer" & 
                                  type_debt == "credit" ~ "Аванси від покупців",
                              debt_owner == "supplier" & 
                                  type_debt == "debit" ~ "Аванси постачальникам товарів",
                              debt_owner == "supplier" & 
                                  type_debt == "credit" ~ "Заборгованість постачальникам товарів",
                              debt_owner == "service_supplier" & 
                                  type_debt == "debit" ~ "Аванси постачальникам послуг",
                              debt_owner == "service_supplier" & 
                                  type_debt == "credit" ~ "Заборгованість постачальникам послуг",
                          )
                      ) %>% 
                      select(date, group, subgroup, sum)
        ) %>% 
        bind_rows(accountable_person_data) %>% 
        bind_rows(payroll_taxes_data %>% 
                      group_by(date, type) %>% 
                      summarise(sum = sum(tax_sum)) %>% 
                      mutate(group = case_when(
                          type == "prepaid" ~ "Дебіторська заборгованість",
                          type == "debt"    ~ "Кредиторська заборгованість"),
                          subgroup = case_when(
                              type == "debt"  ~ "Заборгованість по податкам на ФОП",
                              type == "prepaid" ~ "Аванси інші"
                          )) %>% 
                      select(-type)) %>% 
        bind_rows(payroll_debt_data %>% 
                      group_by(date, type) %>% 
                      summarise(sum = sum(payroll_sum)) %>% 
                      mutate(group = case_when(
                          type == "prepaid" ~ "Дебіторська заборгованість",
                          type == "debt"    ~ "Кредиторська заборгованість"),
                          subgroup = case_when(
                              type == "debt"  ~ "Заборгованість робітникам по ФОП",
                              type == "prepaid" ~ "Аванси інші"
                          )) %>% 
                      select(-type)) %>% 
        bind_rows(taxes_debt_data %>% 
                      group_by(date) %>% 
                      summarise(sum = sum(sum)) %>% 
                      mutate(group = "Заборгованість з податків",
                             subgroup = ""
                      )) %>% 
        bind_rows(inventory_data %>% 
                      mutate(group = "Товарні запаси",
                             subgroup = ""
                      )) %>% 
        mutate(main_group = ifelse(group %in% c("Грошові кошти",
                                                "Дебіторська заборгованість",
                                                "Товарні запаси"),
                                   "Поточні активи",
                                   "Поточні зобов'язання")) %>% 
        group_by(date, main_group, group, subgroup) %>% 
        summarise(sum = sum(sum, na.rm = TRUE)) %>% 
        ungroup() %>% 
        pivot_wider(names_from = "date", values_from = "sum") %>% 
        arrange(main_group, group) %>% 
        select(main_group, group, subgroup, everything()) %>% 
        mutate(across(where(is.numeric), \(x) (replace_na(x, 0)))) %>% 
        mutate(across(where(is.numeric), \(x) round(x/1000, 1)))
    
}


get_working_capital <- function(balance_tbl) {
    
    balance_long_tbl <- balance_tbl %>% 
        pivot_longer(cols = 4:9, names_to = "date", values_to = "sum")
    
    total_current_assets <- balance_long_tbl %>% 
        filter(main_group == "Поточні активи") %>% 
        group_by(date) %>% 
        summarise(assets_sum = sum(sum))
    
    total_current_liabilities <- balance_long_tbl %>% 
        filter(main_group == "Поточні зобов'язання") %>% 
        group_by(date) %>% 
        summarise(liabilities_sum = sum(sum))
    
    working_capital <- total_current_assets %>% 
        full_join(total_current_liabilities, by = "date") %>% 
        mutate(working_capital = assets_sum - liabilities_sum) %>% 
        left_join(balance_long_tbl %>%
                      filter(group == "Товарні запаси") %>% 
                      select(date, inventory = sum),
                  by = "date") %>% 
        mutate(quick_assets = assets_sum - inventory,
               quick_ratio = round(quick_assets / liabilities_sum, 2)) %>% 
        left_join(balance_long_tbl %>%
                      filter(subgroup == "Заборгованість покупців") %>% 
                      select(date, receivables = sum),
                  by = "date")

}


get_pnl_balance_ratios <- function(pnl_data, working_capital) {
    
    pnl_for_ratio <- pnl_data %>% 
        select(1:6) %>% 
        filter(group %in% c("Чистий прибуток", "Дохід від B2B", "Собівартість")) %>% 
        pivot_longer(-group, names_to = "date") %>% 
        mutate(group = case_when(
            group == "Дохід від B2B" ~ "b2b_sale",
            group == "Собівартість" ~ "cost",
            group == "Чистий прибуток" ~ "net_profit"
        )) %>% 
        pivot_wider(names_from = group, values_from = value)
    
    pnl_balance_ratios <- working_capital %>% 
        select(date, working_capital, inventory, receivables) %>% 
        pivot_longer(-date, names_to = "index", values_to = "index_sum") %>% 
        group_by(index) %>% 
        timetk::tk_augment_slidify(
            .value   = index_sum,
            .f       = ~ mean(., na.rm = TRUE),
            .period  = 2,
            .align   = "left",
            .names   = "index_ma"
        ) %>% 
        ungroup() %>% 
        filter(!is.na(index_ma)) %>% 
        select(-index_sum) %>% 
        pivot_wider(names_from = index, values_from = index_ma) %>% 
        mutate(date = paste0(year(date), ":", quarter(date), "кв")) %>% 
        left_join(pnl_for_ratio, by = "date") %>% 
        mutate(inventory_turnover = round(inventory / cost * 90),
               receivable_turnover = round(receivables / b2b_sale * 90, 1),
               working_capital_profitibility = round(net_profit / working_capital, 2))
    
}


get_balance_tbl_finish <- function(balance_tbl) {
    
    balance_tbl_finish <- balance_tbl %>% 
        mutate(across(where(is.numeric), \(x) ifelse(
            main_group == "Поточні зобов'язання",
            -x,
            x))) %>% 
        bind_rows(.,
                  summarise(.,
                            across(where(is.numeric), \(x) sum(x)),
                            across(starts_with("main"), ~ "Обіговий капітал")
                            )
        ) %>%
        mutate(across(where(is.numeric), ~ifelse(
            main_group == "Поточні зобов'язання",
            -.x,
            .x))) %>%
        bind_rows(filter(., main_group == "Поточні активи") %>%
                      summarise(.,
                                across(where(is.numeric), \(x) sum(x)),
                                across(starts_with("main"), ~ "Поточні активи"),
                                across(starts_with("group"), ~ "Всього"))
        ) %>%
        bind_rows(filter(., main_group == "Поточні зобов'язання") %>%
                      summarise(.,
                                across(where(is.numeric), \(x) sum(x)),
                                across(starts_with("main"), ~ "Поточні зобов'язання"),
                                across(starts_with("group"), ~ "Всього")),
        ) %>%
        mutate(across(c(2:3), replace_na, "")) %>% 
        mutate(main_group = factor(main_group,
                                   levels = c("Поточні активи",
                                              "Поточні зобов'язання",
                                              "Обіговий капітал"))) %>% 
        mutate(group = factor(group,
                              levels = c("Всього",
                                         "Грошові кошти",
                                         "Дебіторська заборгованість",
                                         "Товарні запаси",
                                         "Кредиторська заборгованість",
                                         "Заборгованість з податків",
                                         ""))) %>%
        arrange(main_group, group)
    
}
