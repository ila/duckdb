# === Package Installer ===
packages <- c("ggplot2", "dplyr", "ggpubr", "gridExtra", "ggplotify", "readr", "tidyr", "purrr", "stringr")
# Set CRAN mirror
options(repos = c(CRAN = "https://cloud.r-project.org"))

# Install missing packages
install_if_missing <- function(pkg) {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    install.packages(pkg, dependencies = TRUE)
  }
}

install_if_missing("ggplot2")
install_if_missing("dplyr")
install_if_missing("ggpubr")
install_if_missing("gridExtra")
install_if_missing("ggplotify")
install_if_missing("readr")
install_if_missing("tidyr")
install_if_missing("purrr")
install_if_missing("stringr")


installed <- rownames(installed.packages())
for (pkg in packages) {
  if (!(pkg %in% installed)) {
    install.packages(pkg, dependencies = TRUE)
  }
}

library(ggplot2)
library(dplyr)

# Example centralized and decentralized data
# Replace this with your actual data frames
centralized <- read.csv("results_runs.csv")
decentralized <- read.csv("results_daily_runs_city.csv")

# Add source labels
centralized$system <- "Centralized"
decentralized$system <- "Decentralized"

# Combine data
all_data <- bind_rows(centralized, decentralized)

# === AGGREGATE SELECTED QUERIES ===
# Change these later to include/exclude more queries
queries_to_exclude <- c("query1", "query2")

agg_data <- all_data %>%
  filter(!query %in% queries_to_exclude) %>%
  group_by(run, system) %>%
  summarise(time = sum(time), .groups = "drop")

# === PLOT ===
plot_1 <- ggplot(agg_data, aes(x = run, y = time, color = system)) +
  geom_line(size = 1.2) +
  geom_point(size = 2) +
  labs(
    title = paste("Benchmark: ", paste(queries_to_sum, collapse = " + ")),
    x = "Run",
    y = "Total Time (s)",
    color = "System"
  ) +
  theme_minimal(base_size = 14) +
  scale_color_manual(values = c("Centralized" = "#0072B2", "Decentralized" = "#D55E00")) +
  theme(
    plot.title = element_text(face = "bold", hjust = 0.5),
    legend.position = "top"
  )

# Save the plot
png("plots/benchmark_plot.png", width = 800, height = 600)
paste0(plot_1)
dev.off()