library(shiny)
library(bslib)
library(shinyWidgets)
library(shinybusy)

library(tidymodels)
library(modeltime)

library(tidyverse)
library(timetk)
library(plotly)
library(DT)

library(DBI)
library(odbc)
library(dbplyr)

date_finish <- floor_date(Sys.Date(), unit = "month")

source("script/collect_data.R")
source("script/functions.R")


ref_subdiv <- get_ref_subdiv()

old_sale_data <- read_rds("data/old_sale_data.rds")
new_sale_data <- get_sale_total(as.Date("2023-01-01"), date_finish)
sale_data <- bind_rows(old_sale_data, new_sale_data)

check_data <- get_checks_data(as.Date("2017-01-01"), date_finish) %>% 
    mutate(date = as_date(date) - years(2000)) %>% 
    group_by(date) %>% 
    summarise(sale = sum(checks_qty))

dbDisconnect(con_db)


b2c_data <- sale_data %>% 
    filter(project %in% "B2C") %>% 
    mutate(date = date - years(2000)) %>% 
    group_by(date) %>% 
    summarise(sale = sum(sale_sum))

b2b_data <- sale_data %>% 
    filter(project %in% "B2B") %>% 
    mutate(date = date - years(2000)) %>% 
    group_by(date) %>% 
    summarise(sale = sum(sale_sum))

total_sale_data <- sale_data %>% 
    filter(project %in% c("B2B", "B2C")) %>% 
    mutate(date = date - years(2000)) %>% 
    group_by(date) %>% 
    summarise(sale = sum(sale_sum))

b2b_customers_data <- sale_data %>% 
    filter(project %in% "B2B") %>% 
    mutate(date = date - years(2000)) %>% 
    group_by(date) %>% 
    summarise(sale = sum(b2b_customer_qty))

subdiv_data <- sale_data %>% 
    filter(project %in% c("B2B", "B2C"),
           subdiv_id != "000000235") %>%
    left_join(ref_subdiv, by = "subdiv_id") %>% 
    filter(parent_subdiv %in% c("000000133", "000000134") |
               subdiv_id %in% "0000002499",
           subdiv_marked == FALSE) %>% 
    mutate(date = date - years(2000),
           subdiv_name = case_when(
               subdiv_id == "000000189" ~ ref_subdiv$subdiv_name[
                   ref_subdiv$subdiv_id == "000000007"], # Копіцентр ХМ СМ
               subdiv_id == "000000246" &
                   project %in% "B2B" ~ ref_subdiv$subdiv_name[
                       ref_subdiv$subdiv_id == "000000191"], # Луцьк Ковельськ
               .default = subdiv_name
           )) %>% 
    filter(!str_detect(subdiv_category, "(В2С|В2В)") |
               (str_detect(subdiv_category, "В2С") & project %in% "B2C") |
               (str_detect(subdiv_category, "В2В") & project %in% "B2B")) %>% 
    group_by(subdiv_name, date, project) %>% 
    summarise(sale = sum(sale_sum)) %>%
    group_by(subdiv_name, project) %>%
    pad_by_time(.date_var = date,
                .by = "month",
                .end_date = date_finish - months(1),
                .pad_value = 0) %>% 
    ungroup()

current_year <- year(date_finish)
next_quarter <- ceiling_date(Sys.Date(), unit = "quarter") + months(2)
max_date <- as.Date(paste(current_year+1, 12, 1, sep = "-"),
                    "%Y-%m-%d")
cores <- parallel::detectCores()


ui <- page_navbar(
    tags$head(
        tags$style(HTML("
                        .navbar-brand{
                            display:flex;
                        }
                        "))
    ),
    theme = bs_theme(bootswatch ="simplex"),
    title = list(
        tags$image(
            src = "logo.png",
            style = "width:30px;height:30px;margin-right:24px;-webkit-filter:drop-shadow(5px 5px 5px #222);"
        ),
        h5("Прогнозування продаж")
    ),
    id = "navbar",
    fluid = TRUE,
    
    
    sidebar = sidebar(
        conditionalPanel(
            condition = "input.navbar == 'Загальні продажі' ||
                         input.navbar == 'Роздрібні продажі' ||
                         input.navbar == 'Сервісні продажі'",
            width = 250,
            "Прогнозування продаж на обраний період",
            hr(),
            shinyWidgets::airMonthpickerInput(
                inputId = "picker_date",
                label = "Оберіть кінцевий місяць",
                value = next_quarter,
                minDate = Sys.Date(),
                maxDate = max_date
            ),
            hr(),
            actionButton(
                inputId = "apply",
                label   = "Розрахувати",
                icon    = icon("play"),
                class   = "btn-info"
            )
        ),
        conditionalPanel(
            condition = "input.navbar == 'Прогноз для підрозділів'",
            width = 300,
            "Прогнозування продаж на обраний період для обраного підрозділу",
            hr(),
            shinyWidgets::airMonthpickerInput(
                inputId = "picker_date",
                label = "Оберіть кінцевий місяць",
                value = next_quarter,
                minDate = Sys.Date(),
                maxDate = max_date
            ),
            hr(),
            pickerInput(
                inputId = "picker_subdiv",
                label   = "Оберіть підрозділ:",
                choices = sort(unique(subdiv_data$subdiv_name)),
                options = list(
                    size = 5)
            ),
            hr(),
            actionButton(
                inputId = "apply_subdiv",
                label   = "Розрахувати",
                icon    = icon("play"),
                class   = "btn-info"
            )
        )
    ),
    
    nav_panel("Загальні продажі",
              layout_column_wrap(
                  width = NULL,
                  style = htmltools::css(grid_template_columns = "3fr 2fr"),
                  card(
                      #card_header(""),
                      card_body(#textOutput("interval_value")
                          plotlyOutput("plot_total_sale")
                          ),
                      full_screen = FALSE
                  ),
                  card(
                      card_body(DT::dataTableOutput("table_total_sale")),
                      full_screen = FALSE
                      )
                  )
              ),
    
    nav_panel("Роздрібні продажі",
              layout_column_wrap(
                  width = NULL,
                  style = htmltools::css(grid_template_columns = "3fr 2fr"),
                  card(
                       card_header("Продажі, сума"),
                       card_body(plotlyOutput("plot_project_b2c")),
                       full_screen = TRUE
                  ),
                  card(
                       #card_header(""),
                       card_body(DT::dataTableOutput("table_b2c_sale")),
                       full_screen = TRUE
                      )
                  ),
              layout_column_wrap(
                  width = NULL,
                  style = htmltools::css(grid_template_columns = "3fr 2fr"),
                  card(
                      card_header("Продажі, кількість чеків"),
                      card_body(plotlyOutput("plot_checks")),
                      full_screen = TRUE
                  ),
                  card(
                      #card_header(""),
                      card_body(DT::dataTableOutput("table_checks")),
                      full_screen = TRUE
                  )
              )
              ),
    nav_panel("Сервісні продажі",
              layout_column_wrap(
                  width = NULL,
                  style = htmltools::css(grid_template_columns = "3fr 2fr"),
                  card(
                      card_header("Продажі, сума"),
                      card_body(plotlyOutput("plot_project_b2b")),
                      full_screen = TRUE
                  ),
                  card(
                      #card_header("Продажі сервіс"),
                      card_body(DT::dataTableOutput("table_b2b_sale")),
                      full_screen = TRUE
                  )
              ),
              layout_column_wrap(
                  width = NULL,
                  style = htmltools::css(grid_template_columns = "3fr 2fr"),
                  card(
                      card_header("Продажі, кількість покупців"),
                      card_body(plotlyOutput("plot_b2b_customers")),
                      full_screen = TRUE
                  ),
                  card(
                      #card_header("Продажі сервіс"),
                      card_body(DT::dataTableOutput("table_b2b_customers")),
                      full_screen = TRUE
                  )
              )
    ),
    nav_panel("Прогноз для підрозділів",
              layout_column_wrap(
                  width = NULL,
                  style = htmltools::css(grid_template_columns = "3fr 2fr"),
                  card(
                      card_header("Роздрібні продажі, сума"),
                      card_body(plotlyOutput("plot_subdiv_b2c")),
                      full_screen = TRUE
                  ),
                  card(
                      #card_header("Продажі сервіс"),
                      #card_body(verbatimTextOutput("test")),
                      card_body(DT::dataTableOutput("table_subdiv_b2c")),
                      full_screen = TRUE
                  )
              ),
              layout_column_wrap(
                  width = NULL,
                  style = htmltools::css(grid_template_columns = "3fr 2fr"),
                  card(
                      card_header("Сервісні продажі, сума"),
                      card_body(plotlyOutput("plot_subdiv_b2b")),
                      full_screen = TRUE
                  ),
                  card(
                      #card_header("Продажі сервіс"),
                      card_body(DT::dataTableOutput("table_subdiv_b2b")),
                      full_screen = TRUE
                  )
              )
    )
)

# Define server logic required to draw a histogram
server <- function(input, output, session) {
    
    rv <- reactiveValues()

    observeEvent(input$apply, {
        
        show_modal_spinner(
            spin = "cube-grid",
            color = "firebrick",
            text = "Триває розрахунок..."
        )
        
       # rv$last_date <- dim(input$picker_date)
        rv$forecast_horizon <- lubridate::interval(
            date_finish,
            input$picker_date + months(1)) %/% months(1)
        
        parallel_start(cores-1)

        rv$total_sale_prediction <- arima_prediction(total_sale_data,
                                                     rv$forecast_horizon)

        rv$b2c_sale_prediction <- arima_prediction(b2c_data,
                                                   rv$forecast_horizon)
        
        rv$checks_prediction <- xgboost_prediction(check_data,
                                                   rv$forecast_horizon)
        
        rv$b2b_sale_prediction <- arima_prediction(b2b_data,
                                                   rv$forecast_horizon)
        
        rv$b2b_customers_prediction <- xgboost_prediction(b2b_customers_data,
                                                          rv$forecast_horizon)
        
        parallel_stop()
        
        remove_modal_spinner()
        
    }#, once = TRUE
    ) 
    
    
    observeEvent(input$apply_subdiv, {
        
        show_modal_spinner(
            spin = "cube-grid",
            color = "firebrick",
            text = "Триває розрахунок..."
        )
        
        # rv$last_date <- dim(input$picker_date)
        rv$forecast_horizon <- lubridate::interval(
            date_finish,
            input$picker_date + months(1)) %/% months(1)
        
        rv$subdiv_sale <- subdiv_data %>% 
            filter(subdiv_name %in% input$picker_subdiv) %>%
            arrange(date)
        
        rv$subdiv_b2c <- rv$subdiv_sale %>% 
            filter(project %in% "B2C") %>%
            select(date, sale)
        
        parallel_start(cores-1)
        
        rv$subdiv_b2c_prediction <- arima_prediction(
            rv$subdiv_b2c,
            rv$forecast_horizon)
        
        rv$subdiv_b2b <- rv$subdiv_sale %>% 
            filter(project %in% "B2B") %>%
            select(date, sale)

        rv$subdiv_b2b_prediction <- arima_prediction(
            rv$subdiv_b2b,
            rv$forecast_horizon)
        
        parallel_stop()
        
        remove_modal_spinner()
        
    }#, once = TRUE
    ) 
    
    
    output$plot_total_sale <- renderPlotly({

        req(rv$total_sale_prediction)
        
        plot_prediction(rv$total_sale_prediction[[1]])

    })

    
    output$plot_project_b2c <- renderPlotly({

        req(rv$b2c_sale_prediction)
        
        plot_prediction(rv$b2c_sale_prediction[[1]])

    })

    
    output$plot_checks <- renderPlotly({
        
        req(rv$checks_prediction)
        
        plot_prediction(rv$checks_prediction[[1]])

    })
    

    output$plot_project_b2b <- renderPlotly({

        req(rv$b2b_sale_prediction)
        
        plot_prediction(rv$b2b_sale_prediction[[1]])

    })

    
    output$plot_b2b_customers <- renderPlotly({
        
        req(rv$b2b_customers_prediction)
        
        plot_prediction(rv$b2b_customers_prediction[[1]])
        
    })
    

    output$plot_subdiv_b2c <- renderPlotly({
        
        req(rv$subdiv_b2c)
        
        if (nrow(rv$subdiv_b2c) == 0) shiny::validate("Немає даних")
        
        if (nrow(rv$subdiv_b2c) > 0 &
            nrow(rv$subdiv_b2c) < 12) shiny::validate("Недостатньо даних для розрахунку")
        
        plot_prediction(rv$subdiv_b2c_prediction[[1]])
        
    })
    

    output$plot_subdiv_b2b <- renderPlotly({
        
        req(rv$subdiv_b2b)
        
        if (nrow(rv$subdiv_b2b) == 0) shiny::validate("Немає даних")
        
        if (nrow(rv$subdiv_b2b) > 0 &
            nrow(rv$subdiv_b2b) < 12) shiny::validate("Недостатньо даних для розрахунку")
        
        plot_prediction(rv$subdiv_b2b_prediction[[1]])
        
    }) 
    

    output$table_total_sale <- DT::renderDataTable({

        req(rv$total_sale_prediction)

        data_table(rv$total_sale_prediction)
    })

    output$table_b2c_sale <- DT::renderDataTable({

        req(rv$b2c_sale_prediction)
        
        data_table(rv$b2c_sale_prediction)
    })
    
    output$table_checks <- DT::renderDataTable({
        
        req(rv$checks_prediction)
        
        data_table(rv$checks_prediction)
    })

    output$table_b2b_sale <- DT::renderDataTable({

        req(rv$b2b_sale_prediction)
        
        data_table(rv$b2b_sale_prediction)
    })
    
    output$table_b2b_customers <- DT::renderDataTable({
        
        req(rv$b2b_customers_prediction)
        
        data_table(rv$b2b_customers_prediction)
    })
    
    
    output$table_subdiv_b2c <- DT::renderDataTable({
        
        req(rv$subdiv_b2c_prediction)
        
        data_table(rv$subdiv_b2c_prediction)
    })
    
    output$table_subdiv_b2b <- DT::renderDataTable({
        
        req(rv$subdiv_b2b_prediction)
        
        data_table(rv$subdiv_b2b_prediction)
    })
    
}

# Run the application 
shinyApp(ui = ui, server = server)