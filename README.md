Overview

This notebook/script (FAM_Event_N_Member_Features.py) orchestrates the full automated feature engineering pipeline for the Fraud Authentication Model (FAM).

The pipeline generates features across multiple interaction touchpoints:

Category

Data Source (Session Type)

Feature Output Tables

Phone

Phone session tracking

<segment>_PHONE_1D/7D/30D

Email

Email session tracking

<segment>_EMAIL_1D/7D/30D

Address

Address change events

<segment>_ADDR_1D/7D/30D

CAP

Customer Access Portal (CAP)

<segment>_CAP_1D/7D/30D

IP

IP session tracking

<segment>_IP_1D/7D/30D

Glassbox

Device ID / Machine ID / Visitor ID

<segment>_GLASSBOX_*

Member Tables

Digital & Cross-dimension joins

<segment>_MEMBER

Each feature category is independently generated, saved into Snowflake tables, and then merged to the final feature tables used in training / inference.

Inputs / Configuration

All input configuration is externalized in YAML files, allowing the pipeline to run across segments:

DIGITAL:
  yam: "/mnt/.../digital_train_pipeline_config.yaml"
  key: "DEST"

TELEPHONE:
  yam: "/mnt/.../telephone_train_pipeline_config.yaml"
  key: "DEST"


Each YAML file defines:

Config Key

Description

base_table_with_labels

Joined label + raw event dataset for ML

*_session_table

Session-level detail table

*_output_table

Final destination table for features

start_date, end_date

Event window cutoffs

look_back_window

Max backward window (>= 60 days)

hash_id

Entity grain to join features (member or event)

Execution Flow

1. Load Segment Configurations

Each segment (Digital, Telephone) iterates independently:

for segment, cfg in segment_configs.items():
    load config yaml


2. Generate Feature Groups

For each feature group (Phone, Email, Address, CAP, IP):

fam_phone.phone_features(
   base_table, session_table,
   sessionsession, output_table,
   start_date, end_date,
   lookback_window, n_days_list=[1, 7, 30, 60],
   key=...
)


Purpose: create rolling aggregated features such as:

Each feature category runs independently and prints completion status.

3. Glassbox Features

Glassbox generates features for three separate entity grains:

id_types = ["DEVICE_ID", "MACHINE_ID", "VISITOR_ID"]


The pipeline maps:

"DEVICE_ID" -> GLASSBOX_DEVICE_OUTPUT_TABLE
"MACHINE_ID" -> GLASSBOX_MACHINE_OUTPUT_TABLE
"VISITOR_ID" -> GLASSBOX_VISITOR_OUTPUT_TABLE


Each produces output with hierarchy-aware aggregations (important for fraud device tracing).

4. Member-Level Feature Generation

Member feature pipeline combines multiple member tables (digital + cross-dimension):

member_feature_configs = [
  {"right_table_prefix": dig_member_table, "output_table": dig_member_output},
  {"right_table_prefix": dig_member_crossdim_table, "output_table": dig_member_crossdim_output},
  ...
]


For each configuration:

Extract features at 1D / 7D / 30D windows

Feature selection occurs (top_n based on importance score)

Write results into Snowflake

Example output:

DIG_MEMBER_1D
DIG_MEMBER_7D
DIG_MEMBER_30D


5. Merge All Outputs into Final Member Feature Table

Final merge SQL:

SELECT * FROM temp_outputs[0]
LEFT JOIN temp_outputs[1] USING (hash_id)
LEFT JOIN temp_outputs[2] USING (hash_id)


Saved into final configured Snowflake table:

<segment>_MEMBER_FEATURES


Completion Log Output

The pipeline prints progress automatically:

Processing segment: DIGITAL
Completed segment for Phone
Completed segment for Email
Completed segment for Address
Completed segment for CAP
Completed segment for IP
Completed segment for Glassbox
Completed merge segment: DIGITAL


Output Artifacts (for Model Training)

Output Table

Description

<segment>_PHONE

Phone update features

<segment>_EMAIL

Email update features

<segment>_ADDRESS

Address update features

<segment>_CAP

Customer portal behavior

<segment>_IP

IP risk / geo changes / velocity

<segment>_GLASSBOX_DEVICE/MACHINE/VISITOR

Device identity fingerprinting

<segment>_MEMBER_FEATURES

Selected member features

<segment>_FINAL

Unified feature dataset

Benefits

Benefit

Value

Config-driven

new segments enabled without code changes

Parallel feature generation

modular, scalable

Rolling window aggregation

short- vs long-term behaviour

Feature selection included

reduces noise & dimensionality

Fully automated write-back to Snowflake

consistent feature lineage

Execution Flow / Dependencies

         ┌────────────────────────────────────────────┐
         │      YAML Configuration (per segment)  │
         │ digital_train_pipeline.yaml            │
         │ telephone_train_pipeline.yaml          │
         └───────────────┬────────────────────────────┘
                         │
                         ▼
                 Load Base Table
           (base_table_with_labels)

                         │
 ┌─────────────Feature Engineering Orchestration Loop──────────────┐
 │ for segment in [DIGITAL, TELEPHONE]:                          │
 │                                                               │
 │   ┌───────────Phone────────────┐                                │
 │   │ phone_session_table        │                              │
 │   └──────▶ phone_features() ───┘                              │
 │   ┌───────────Email────────────┐                                │
 │   │ email_session_table        │                              │
 │   └──────▶ email_features() ───┘                              │
 │   ┌──────────Address───────────┐                                │
 │   │ address_session_table      │                              │
 │   └────▶ address_features() ───┘                              │
 │   ┌────────────CAP─────────────┐                                │
 │   │ cap_session_table          │                              │
 │   └────────▶ cap_features() ───┘                              │
 │   ┌────────────IP──────────────┐                                │
 │   │ ip_session_table           │                              │
 │   └────────▶ ip_features() ────┘                               │
 │   ┌──────────Glassbox──────────┐                                │
 │   │ glassbox_session_table     │                              │
 │   └─────▶ glassbox_features()──┘                              │
 │
 │   ▼
 │ Merge temporary outputs (1D, 7D, 30D)
 │ temp_outputs[0] ── JOIN ── temp_outputs[1] ── JOIN ── temp_outputs[2]
 │
 │   ▼
 │ Save to final unified feature table:
 │       <segment>_MEMBER_FEATURES
 │
 └──────────────────────────────────────────────────────────────────┘

