# === Package Installer ===
packages <- c("ggplot2", "dplyr", "ggpubr", "gridExtra", "ggplotify", "readr", "tidyr", "purrr", "stringr", "extrafont", "scales", "knitr", "cowplot")
# Set CRAN mirror
options(repos = c(CRAN = "https://cloud.r-project.org"))

installed <- rownames(installed.packages())
for (pkg in packages) {
  if (!(pkg %in% installed)) {
    install.packages(pkg, dependencies = TRUE)
  }
}

library(ggplot2)
library(dplyr)
library(ggpubr)
library(gridExtra)
library(ggplotify)
library(readr)
library(tidyr)
library(purrr)
library(stringr)
library(extrafont)
library(scales)
library(knitr)
library(cowplot)

# font_import()


summarize_queries_to_latex <- function(df) {
  # Aggregate time by system and query_hash
  summary_df <- df %>%
    filter(run == 4) %>%
    group_by(system, query_hash) %>%
    summarise(`Time (ms)` = sum(time_ms), .groups = "drop") %>%
    group_by(system) %>%
    mutate(percent = round(100 * `Time (ms)` / sum(`Time (ms)`), 2)) %>%
    filter(percent >= 5) %>%
    arrange(system, desc(percent)) %>%
    rename(Query = query_hash) %>%
    rename(`Time (%)` = percent) %>%
    ungroup()

  for (sys in unique(summary_df$system)) {
    sys_df <- summary_df %>%
      filter(system == sys) %>%
      select(-system)  # Remove system column

    # convert sys to lowercase and remove trailing and leading spaces
    sys <- tolower(str_trim(sys))

    latex_table <- kable(
      sys_df,
      format = "latex",
      booktabs = TRUE,
      caption = paste0("Breakdown of queries for ``", sys, "\", run 4, > 5\\% total time"),
      label = tolower(sys)
    )

    # Replace \begin{table} with \begin{table}[h]
    latex_table <- gsub("\\\\begin\\{table\\}", "\\\\begin{table}[h]", latex_table)

    cat(paste0("% ---- Table for system: ", sys, " ----\n"))
    cat(latex_table)
    cat("\n\n")
    }

}

# Example centralized and decentralized data
# Replace this with your actual data frames
centralized_1 <- read.csv("results/results_runs_1.csv")
centralized_3 <- read.csv("results/results_runs_3.csv")
decentralized <- read.csv("results/results_daily_steps_user.csv")
decentralized_upsert <- read.csv("results/results_daily_steps_user_upsert_min_agg_100_all.csv")
decentralized_min_agg <- read.csv("results/results_daily_steps_user_min_agg_100_all.csv")
decentralized_min_agg_90 <- read.csv("results/results_daily_steps_user_min_agg_100_90.csv")
decentralized_refresh_4 <- read.csv("results/results_daily_steps_user_refresh_4_min_agg_100.csv")
decentralized_refresh_2 <- read.csv("results/results_daily_steps_user_refresh_2_min_agg_100.csv")
decentralized_window_4 <- read.csv("results/results_daily_steps_user_window_6_min_agg_100.csv")
decentralized_window_2 <- read.csv("results/results_daily_steps_user_window_12_min_agg_100.csv")

# cpu data
cpu_decentralized <- read.csv("results/cpu_decentralized.csv")
cpu_centralized_10 <- read.csv("results/cpu_centralized_10.csv")
cpu_centralized_30 <- read.csv("results/cpu_centralized_30.csv")
cpu_centralized_100 <- read.csv("results/cpu_centralized_100.csv")

# Add source labels
centralized_1$system <- "Centralized, 1 run/day"
centralized_3$system <- "Centralized, 3 runs/day"
decentralized$system <- "Decentralized"
decentralized_min_agg$system <- "Dec., min. agg. 100, 100% selectivity"
decentralized_min_agg_90$system <- "Dec., min. agg. 100, 90% selectivity"
decentralized_upsert$system <- "UPSERT, 1 refresh/window"
#decentralized_upsert$system <- "Dec., UPSERT, min. agg. 100, 100% selectivity"
decentralized_refresh_4$system <- "4 refreshes/window"
decentralized_refresh_2$system <- "2 refreshes/window"
decentralized_window_4$system <- "Window of 6 hours"
decentralized_window_2$system <- "Window of 12 hours"

# cpu data
cpu_decentralized$system <- "Decentralized"
cpu_centralized_10$system <- "Centralized, 10 data points/day"
cpu_centralized_30$system <- "Centralized, 30 data points/day"
cpu_centralized_100$system <- "Centralized, 100 data points/day"

decentralized_refresh_4_grouped <- decentralized_refresh_4 %>%
  mutate(run = run %/% 4) %>%  # creates groups: 1–4 -> 1, 5–8 -> 2, etc.
  group_by(run, system) %>%
  summarise(time_ms = sum(time_ms))

decentralized_refresh_2_grouped <- decentralized_refresh_2 %>%
  mutate(run = run %/% 2) %>%
  group_by(run, system) %>%
  summarise(time_ms = sum(time_ms))

decentralized_window_4_grouped <- decentralized_window_4 %>%
  mutate(run = run %/% 4) %>%
  group_by(run, system) %>%
  summarise(time_ms = sum(time_ms))

decentralized_window_2_grouped <- decentralized_window_2 %>%
    mutate(run = run %/% 2) %>%
    group_by(run, system) %>%
    summarise(time_ms = sum(time_ms))

# === AGGREGATE SELECTED QUERIES ===
# Change these to include/exclude more queries
# 273e78cb -> detach parser
# c3c4ce2d -> attach parser
# 46f37273 -> attach parser read only
# e1e93656 -> attach client
queries_to_exclude <- c("273e78cb", "46f37273", "e1e93656", "c3c4ce2d")
# buffer size, responsiveness, completeness
metadata_queries <- c("e3d02a6a", "6245fe4e", "56db5fd8")

# Combine data
all_data_1 <- bind_rows(centralized_1, centralized_3, decentralized)
all_data_2 <- bind_rows(centralized_1, decentralized_min_agg, decentralized_min_agg_90, decentralized_upsert)
all_data_3 <- bind_rows(decentralized_upsert, decentralized_refresh_4_grouped, decentralized_refresh_2_grouped)
all_data_4 <- bind_rows(decentralized_upsert, decentralized_window_4_grouped, decentralized_window_2_grouped)
all_data_cpu <- bind_rows(cpu_decentralized, cpu_centralized_10, cpu_centralized_30, cpu_centralized_100)

all_data_1 <- all_data_1 %>%
  filter(!as.character(query_hash) %in% queries_to_exclude)

agg_data_1 <- all_data_1 %>%
  group_by(run, system) %>%
  summarise(total_time = sum(time_ms), .groups = "drop")

agg_data_2 <- all_data_2 %>%
  filter(!as.character(query_hash) %in% queries_to_exclude)

agg_data_2 <- all_data_2 %>%
  group_by(run, system) %>%
  summarise(total_time = sum(time_ms), .groups = "drop")

all_data_3 <- all_data_3 %>%
  filter(!as.character(query_hash) %in% queries_to_exclude)

agg_data_3 <- all_data_3 %>%
  group_by(run, system) %>%
  summarise(total_time = sum(time_ms), .groups = "drop")

all_data_4 <- all_data_4 %>%
  filter(!as.character(query_hash) %in% queries_to_exclude)

agg_data_4 <- all_data_4 %>%
  group_by(run, system) %>%
  summarise(total_time = sum(time_ms), .groups = "drop")

# summarize_queries_to_latex(all_data_2)

cb_palette <- c(
  "Centralized, 10 data points/day" = "#332288",
  "Centralized, 30 data points/day" = "#A755C2",
  "Centralized, 100 data points/day" = "#88CCEE",
  "Decentralized" = "#C51310",
  "Dec., min. agg. 100, 100% selectivity" = "#f1a66a",
  "Dec., min. agg. 100, 90% selectivity" = "#FF3366",
  "Dec., UPSERT, min. agg. 100, 100% selectivity" = "#2ec4b6",
  "UPSERT, 1 refresh/window" = "#2ec4b6"
)

cb_shapes <- c(
  "Centralized, 10 data points/day" = 18,  # diamond
  "Centralized, 30 data points/day" = 17,  # triangle
  "Centralized, 100 data points/day" = 16,  # square
  "Decentralized" = 15,  # square
  "Dec., min. agg. 100, 100% selectivity" = 16,  # circle
  "Dec., min. agg. 100, 90% selectivity" = 10,  # small circle
  "Dec., UPSERT, min. agg. 100, 100% selectivity" = 8,  # asterisk
  "UPSERT, 1 refresh/window" = 8  # asterisk
)

# === PLOT ===
plot_1 <- ggplot(agg_data_1, aes(x = run, y = total_time, color = system, shape = system)) +
  geom_line(linewidth = 1.2) +
  geom_point(size = 3) +
  scale_color_manual(values = cb_palette) +
  scale_shape_manual(values = cb_shapes) +
  labs(x = "Run", y = "Total Time (ms)", color = "System", shape = "System") +
  scale_y_continuous(labels = scales::comma, breaks = seq(0, 8000, by = 1000)) +
  theme_minimal(base_size = 14, base_family = "Linux Libertine") +
  theme(
    plot.title = element_text(face = "bold", hjust = 0.5),
    legend.position = "top",
    legend.margin = margin(c(0, 0, 0, -50)),
    axis.text.x = element_text(size = 14),
    axis.text.y = element_text(size = 14)
  )


# Save the plot
png("plots/centralized_vs_decentralized_plot.png", width = 2000, height = 1000, res = 350)
print(plot_1)
dev.off()

plot_2 <- ggplot(agg_data_2, aes(x = run, y = total_time, color = system, shape = system)) +
  geom_line(linewidth = 1.2) +
  geom_point(size = 3) +
  labs(x = "Run", y = "Total Time (ms)", color = "System", shape = "System") +
  scale_color_manual(values = cb_palette) +
  scale_shape_manual(values = cb_shapes) +
  scale_y_continuous(labels = scales::comma, breaks = seq(0, 8000, by = 1000)) +
  theme_minimal(base_size = 14, base_family = "Linux Libertine") +
  theme(
    plot.title = element_text(face = "bold", hjust = 0.5),
    legend.position = "top",
    legend.margin = margin(c(0, 0, 0, -50)),
    axis.text.x = element_text(size = 14),
    axis.text.y = element_text(size = 14),
    legend.text = element_text(size = 9)
  ) +
  guides(color = guide_legend(ncol = 2))

# Save the plot
png("plots/minimum_aggregation_plot.png", width = 2000, height = 1100, res = 350)
print(plot_2)
dev.off()

plot_3 <- ggplot(agg_data_3, aes(x = run, y = total_time, color = system, shape = system)) +
  geom_line(linewidth = 1.2) +
  geom_point(size = 3) +
  labs(x = "Run", y = "Total Time (ms)", color = "System", shape = "System") +
  #scale_color_manual(values = cb_palette) +
  #scale_shape_manual(values = cb_shapes) +
  scale_y_continuous(labels = scales::comma, breaks = seq(0, 8000, by = 1000)) +
  theme_minimal(base_size = 14, base_family = "Linux Libertine") +
  theme(
    plot.title = element_text(face = "bold", hjust = 0.5),
    legend.position = "top",
    legend.margin = margin(c(0, 0, 0, -50)),
    axis.text.x = element_text(size = 14),
    axis.text.y = element_text(size = 14),
    legend.text = element_text(size = 9)
  )

# Save the plot
png("plots/refresh_plot.png", width = 2000, height = 1100, res = 350)
print(plot_3)
dev.off()


plot_4 <- ggplot(agg_data_4, aes(x = run, y = total_time, color = system, shape = system)) +
  geom_line(linewidth = 1.2) +
  geom_point(size = 3) +
  labs(x = "Run", y = "Total Time (ms)", color = "System", shape = "System") +
  #scale_color_manual(values = cb_palette) +
  #scale_shape_manual(values = cb_shapes) +
  scale_y_continuous(labels = scales::comma, breaks = seq(0, 8000, by = 1000)) +
  theme_minimal(base_size = 14, base_family = "Linux Libertine") +
  theme(
    plot.title = element_text(face = "bold", hjust = 0.5),
    legend.position = "top",
    legend.margin = margin(c(0, 0, 0, -50)),
    axis.text.x = element_text(size = 14),
    axis.text.y = element_text(size = 14),
    legend.text = element_text(size = 9)
  )

# Save the plot
png("plots/window_plot.png", width = 2000, height = 1100, res = 350)
print(plot_4)
dev.off()


# cpu plots
plot_5 <- ggplot(all_data_cpu, aes(x = run, y = avg_system_cpu, color = system, shape = system)) +
  geom_line(linewidth = 1.2) +
  geom_point(size = 3) +
  labs(x = "Benchmark Run", y = "Average CPU Usage (%)", color = "System", shape = "System") +
  scale_color_manual(values = cb_palette) +
  scale_shape_manual(values = cb_shapes) +
  scale_y_continuous(labels = scales::comma, breaks = seq(0, 100, by = 10)) +
  theme_minimal(base_size = 14, base_family = "Linux Libertine") +
  theme(
    plot.title = element_text(face = "bold", hjust = 0.5),
    legend.position = "top",
    legend.margin = margin(c(3, 3, 3, 3)),
    axis.text.x = element_text(size = 14),
    axis.text.y = element_text(size = 14),
    legend.text = element_text(size = 9)
  ) +
    guides(color = guide_legend(ncol = 2,
                                override.aes = list(size = 2),  # Smaller legend symbols
                                keywidth = unit(0.8, "cm"),    # Narrower legend keys
                                keyheight = unit(0.1, "cm")))  # Shorter legend keys


# Save the plot
png("plots/cpu_plot.png", width = 2000, height = 1000, res = 350)
print(plot_5)
dev.off()

plot_6 <- ggplot(all_data_cpu, aes(x = run, y = storage_size_bytes / 1000000000, color = system, shape = system)) +
  geom_line(linewidth = 1.2) +
  geom_point(size = 3) +
  labs(x = "Benchmark Run", y = "Storage Space (GB)", color = "System", shape = "System") +
  scale_color_manual(values = cb_palette) +
  scale_shape_manual(values = cb_shapes) +
  scale_y_continuous(labels = scales::comma, breaks = seq(0, 10, by = 0.2)) +
  theme_minimal(base_size = 14, base_family = "Linux Libertine") +
  theme(
    plot.title = element_text(face = "bold", hjust = 0.5),
    legend.position = "top",
    legend.margin = margin(c(3, 3, 3, 3)),
    axis.text.x = element_text(size = 14),
    axis.text.y = element_text(size = 14),
    legend.text = element_text(size = 9)
  ) +
    guides(color = guide_legend(ncol = 2,
                                override.aes = list(size = 2),  # Smaller legend symbols
                                keywidth = unit(0.8, "cm"),    # Narrower legend keys
                                keyheight = unit(0.1, "cm")))  # Shorter legend keys


# Save the plot
png("plots/storage_plot.png", width = 2000, height = 1000, res = 350)
print(plot_6)
dev.off()

plot_7 <- ggplot(all_data_cpu, aes(x = run, y = bytes_received / 1000000000, color = system, shape = system)) +
  geom_line(linewidth = 1.2) +
  geom_point(size = 3) +
  labs(x = "Benchmark Run", y = "Data Received (GB)", color = "System", shape = "System") +
  scale_color_manual(values = cb_palette) +
  scale_shape_manual(values = cb_shapes) +
  scale_y_continuous(labels = scales::comma, breaks = seq(0, 10, by = 0.5)) +
  theme_minimal(base_size = 14, base_family = "Linux Libertine") +
  theme(
    plot.title = element_text(face = "bold", hjust = 0.5),
    legend.position = "top",
    legend.margin = margin(c(3, 3, 3, 3)),
    axis.text.x = element_text(size = 14),
    axis.text.y = element_text(size = 14),
    legend.text = element_text(size = 9)
  ) +
    guides(color = guide_legend(ncol = 2,
                                override.aes = list(size = 2),  # Smaller legend symbols
                                keywidth = unit(0.8, "cm"),    # Narrower legend keys
                                keyheight = unit(0.1, "cm")))  # Shorter legend keys


# Save the plot
png("plots/network_plot.png", width = 2000, height = 1000, res = 350)
print(plot_7)
dev.off()

# Remove legends from individual plots
# plot_5 <- plot_5 + theme(legend.position = "none")
# plot_6 <- plot_6 + theme(legend.position = "none")
# plot_7 <- plot_7 + theme(legend.position = "none")



# Save to file
png("plots/combined_plot.png", width = 2000, height = 1000, res = 350)
ggarrange(plot_5, plot_6, plot_7, ncol=3, nrow=1, common.legend = TRUE, legend="bottom")
dev.off()