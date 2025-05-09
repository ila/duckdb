library(ggplot2)
library(dplyr)

# Example centralized and decentralized data
# Replace this with your actual data frames
centralized <- data.frame(
  run = rep(1:5, each = 3),
  query = rep(c("query1", "query2", "query3"), times = 5),
  time = runif(15, 1, 10)
)

decentralized <- data.frame(
  run = rep(1:5, each = 3),
  query = rep(c("query1", "query2", "query3"), times = 5),
  time = runif(15, 2, 12)
)

# Add source labels
centralized$system <- "Centralized"
decentralized$system <- "Decentralized"

# Combine data
all_data <- bind_rows(centralized, decentralized)

# === AGGREGATE SELECTED QUERIES ===
# You can change these later to include/exclude more queries
queries_to_sum <- c("query1", "query2")

agg_data <- all_data %>%
  filter(query %in% queries_to_sum) %>%
  group_by(run, system) %>%
  summarise(time = sum(time), .groups = "drop")

# === PLOT ===
ggplot(agg_data, aes(x = run, y = time, color = system)) +
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
