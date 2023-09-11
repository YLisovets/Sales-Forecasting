arima_prediction <- function(data, forecast_horizon) {
    
    if (nrow(data) < 12) return(NULL)

    # splits <- initial_time_split(data, prop = 0.9)
    splits <- time_series_split(data,
                                assess = paste0(forecast_horizon, " months"),
                                cumulative = TRUE)

    df_train <- training(splits)
    df_test <- testing(splits)
    df_folds <- time_series_cv(df_train, 
                               assess      = paste0(forecast_horizon, " months"),
                               skip        = "1 months",
                               cumulative  = TRUE,
                               slice_limit = 10)
    df_folds %>%
        tk_time_series_cv_plan() %>%
        plot_time_series_cv_plan(date, sale,
                                 .facet_ncol = 2,
                                 .interactive =  TRUE)
    
    # Model arima_boost 
    model_arima_boost <- 
        arima_boost(
            min_n = tune(),
            learn_rate = tune(),
            trees = tune()
        ) %>%
        set_engine(engine = "auto_arima_xgboost")
    
    arima_rec <- recipe(sale ~ date, data = df_train) %>% 
        step_timeseries_signature(date) %>%
        step_rm(matches("(.xts$)|(.iso$)|(hour)|(minute)|(second)|(day)|(week)|(am.pm)")) %>%
        step_normalize(contains("index.num"), date_year) %>%
        step_zv(all_predictors()) %>% 
        step_dummy(contains("lbl"), one_hot = TRUE) 
    
    wflw_arima_boost <- workflow() %>% 
        add_model(model_arima_boost) %>% 
        add_recipe(arima_rec)
    
    #Tuning and evaluating model
    set.seed(1900)
    grid_res <- tune_grid(
        wflw_arima_boost,
        resamples = df_folds,
        grid = 10,
        metrics = metric_set(mpe),
        control = control_grid(save_pred = TRUE,
                               parallel_over = "everything",
                               save_workflow = TRUE,
                               allow_par =  TRUE)
    )
    
    arima_boost_best_param <- grid_res %>%
        select_best(metric = "mpe")
    
    arima_boost_fit_wflw <- 
        finalize_workflow(wflw_arima_boost, arima_boost_best_param) %>% 
        fit(df_train)
    
    #Add fitted model to a Model Table
    models_tbl_arima_boost <-
        modeltime_table(arima_boost_fit_wflw)

    #Calibrate the model to the testing set
    calibration_tbl_arima_boost <-
        models_tbl_arima_boost %>%
        modeltime_calibrate(new_data = df_test)
    
    test_mape <- calibration_tbl_arima_boost %>%
        modeltime_accuracy() %>% 
        pull(mape) %>% round(1)

    #Refit to Full Dataset
    refit_tbl_arima_boost <-
        calibration_tbl_arima_boost %>%
        modeltime_refit(data = data)
    
    
    prediction <- refit_tbl_arima_boost %>%
        modeltime_forecast(h = paste0(forecast_horizon, " months"), 
                           actual_data = data,
                           conf_interval = 0.8)
    
    return(list(prediction, test_mape))
}


xgboost_prediction <- function(data, forecast_horizon) {
    
    roll <- c(3, 6, 12)
    
    full_data_tbl <- data %>%
        
        # Apply Group-wise Time Series Manipulations
        future_frame(
            .date_var   = date,
            .length_out = forecast_horizon,
            .bind_data  = TRUE
        ) %>%
        #left_join(total_working_time, by = "date") %>% 
        
        # APPLY TIME SERIES FEATURE ENGINEERING 
        tk_augment_fourier(.date_var = date, .periods = 12, .K = c(1,2)) %>%
        tk_augment_lags(sale, .lags = 12,
                        .names = str_c("lag_", 12)) %>%
        tk_augment_slidify(
            contains("lag_12"),
            .f       = ~ mean(., na.rm = TRUE),
            .period  = 12,
            .align   = "right",
            .partial = TRUE,
            .names = str_c("MA_12_", 12)
        ) %>%
        tk_augment_slidify(
            contains("lag_12"),
            .f       = ~ sd(., na.rm = TRUE),
            .period  = roll,
            .align   = "right",
            .partial = TRUE,
            .names = str_c("SD_12_", roll)
        )
    
    data_prepared_tbl <- full_data_tbl %>% 
        filter(!is.na(sale)) %>%
        filter(!is.na(SD_12_3))
    
    future_data_tbl <- full_data_tbl %>%
        filter(is.na(sale))
    
    # splits <- initial_time_split(data_prepared_tbl, prop = 0.9)
    splits <- time_series_split(data_prepared_tbl,
                                assess = paste0(forecast_horizon, " months"),
                                cumulative = TRUE)
    
    df_train <- training(splits)
    df_test <- testing(splits)
    df_folds <- time_series_cv(df_train, 
                               assess      = paste0(forecast_horizon, " months"),
                               skip        = "1 months",
                               cumulative  = TRUE,
                               slice_limit = 10)
    
    # Model arima_boost 
    model_xgboost <- 
        boost_tree(
            mode = "regression",
            trees       = tune(),
            stop_iter   = tune(),
            learn_rate  = tune(),
            min_n       = tune(),
            tree_depth  = tune(),
            sample_size = tune()
        ) %>%
        set_engine(engine = "xgboost")
    
    spec_rec <- recipe(sale ~ date, data = df_train) %>% 
        step_timeseries_signature(date) %>%
        step_rm(matches("(.xts$)|(.iso$)|(hour)|(minute)|(second)|(day)|(week)|(am.pm)")) %>%
        step_rm(c(date, date_month)) %>%
        step_normalize(contains("index.num"), date_year) %>%
        step_zv(all_predictors()) %>% 
        step_dummy(contains("lbl"), one_hot = TRUE) 
    
    wflw_xgboost <- workflow() %>% 
        add_model(model_xgboost) %>% 
        add_recipe(spec_rec)
    
    #Tuning and evaluating model
    set.seed(1950)
    grid_res <- tune_grid(
        wflw_xgboost,
        resamples = df_folds,
        grid = 15,
        metrics = metric_set(rmse),
        control = control_grid(save_pred = TRUE,
                               parallel_over = "everything",
                               save_workflow = TRUE,
                               allow_par =  TRUE)
    )
    
    xgboost_best_param <- grid_res %>%
        select_best(metric = "rmse")
    
    xgboost_fit_wflw <- 
        finalize_workflow(wflw_xgboost, xgboost_best_param) %>% 
        fit(df_train)
    

    #Add fitted model to a Model Table
    model_tbl <- 
        modeltime_table(xgboost_fit_wflw)
    
    #Calibrate the model to the testing set
    calibration_tbl <-  model_tbl %>%
        modeltime_calibrate(new_data = df_test)
    
    test_mape <- calibration_tbl %>%
        modeltime_accuracy() %>% 
        pull(mape) %>% round(1)
    
    #Refit to Full Dataset 
    refit_tbl_xgboost <- calibration_tbl %>%
        modeltime_refit(data = data_prepared_tbl)
    
    
    prediction <- refit_tbl_xgboost %>%
        modeltime_forecast(new_data = future_data_tbl,
                           actual_data = data_prepared_tbl,
                           conf_interval = 0.8)
    
    return(list(prediction, test_mape))
}


plot_prediction <- function(data) {
    
    plot_prediction_static <- data %>%
        mutate(.index = as_date(.index)) %>% 
        rename("Місяць" = .index,
               "Мин"    = .conf_lo,
               "Макс"   = .conf_hi) %>% 
        timetk::plot_time_series(
            .date_var     = Місяць,
            .value        = .value,
            .color_var    = .model_desc,
            .line_size    = 0.6,
            .title        = "",
            .smooth       = FALSE,
            .legend_show  = FALSE,
            .interactive  = FALSE
        ) +
        scale_y_continuous(labels = scales::label_number(
            scale_cut = scales::cut_short_scale())) +
        scale_x_date(date_breaks = "1 year", date_labels = "%Y") +
        scale_color_manual(values = c("black", "steelblue")) +
        ggplot2::geom_ribbon(
            ggplot2::aes(
                ymin = Мин,
                ymax = Макс,
                color = .model_desc
            ),
            fill     = "grey20",
            alpha    = 0.20,
            linetype = 0
        )
    
    plotly::ggplotly(plot_prediction_static,
                     tooltip = c("x", "y", "ymin", "ymax"))
    
}

data_table <- function(data) {
    
    data[[1]] %>% filter(.key == "prediction") %>%
        transmute(Місяць   = format(.index, "%m-%Y"),
                  Прогноз  = .value,
                  Мінімум  = .conf_lo,
                  Максимум = .conf_hi) %>%
        datatable(rownames = FALSE,
                  caption = paste0("Похибка прогнозу у попередньому періоді - ",
                                   data[[2]], "%"),
                  options = list(dom = 't',
                                 pageLength = 10)) %>%
        formatRound(2:4, digits = 0, mark = "'") %>%
        formatStyle(2,  backgroundColor = '#DFEFFA', fontWeight = 'bold') %>%
        formatStyle(4,  backgroundColor = '#E5F2D0') %>%
        formatStyle(3,  backgroundColor = '#FFDB99')
    
}
