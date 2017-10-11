CREATE TABLE liquibase.dim_product (
  product_key BIGINT,
  sku STRING,
  product_creation_date_key INT,
  product_creation_time_key STRING,
  product_publish_date_key INT,
  product_publish_time_key STRING,
  product_launch_date_key INT,
  product_launch_time_key STRING,
  product_retirement_date_key INT,
  product_retirement_time_key STRING,
  product_title STRING,
  product_type_key BIGINT,
  product_color_key BIGINT,
  product_vendor_size STRING,
  product_vendor STRING,
  product_subvendor STRING,
  product_description STRING,
  product_size_tags STRING,
  product_new_arrival_tags STRING,
  product_tags STRING,
  product_inventory_state_key BIGINT,
  scd_start STRING,
  scd_end STRING,
  health_status INT,
  product_handle STRING,
  retirement_flag INT,
  dropship_flag INT,
  source STRING,
  tenant_key INT
)